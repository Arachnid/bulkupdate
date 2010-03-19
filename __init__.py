import datetime
import logging
import sys
import time
import traceback
from google.appengine.api import mail
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext.deferred import defer
from google.appengine.runtime import apiproxy_errors

from bulkupdate import model


class BulkUpdater(object):
  """A bulk updater for datastore entities.
  
  Subclasses should implement, at a minimum, get_query and handle_entity.
  """

  # Number of entities to put() at once.
  PUT_BATCH_SIZE = 20
  
  # Number of entities to delete() at once.
  DELETE_BATCH_SIZE = 100
  
  # Maximum time to spend processing before enqueueing the next task in seconds.
  MAX_EXECUTION_TIME = 20.0
  
  # Maximum number of failures to tolerate before aborting. -1 indicates
  # no limit, in which case the list of failed keys will not be retained.
  MAX_FAILURES = 0
  
  def __init__(self):
    self.__to_put = []
    self.__to_delete = []
    self.__log_entries = []
    self._status = model.Status()
    self.task_id = 0
    self.current_key = None

  def __getstate__(self):
    state = dict(self.__dict__)
    status = state['_status']
    if not status.is_saved():
      status.put()
    state['_status'] = status.key()
    return state

  def __setstate__(self, state):
    state['_status'] = db.get(state['_status'])
    self.__dict__.update(state)
  
  def get_query(self):
    """Returns the query to iterate over.

    Returns:
      A db.Query or db.GqlQuery object. The returned query must support cursors.
    """
    raise NotImplementedError()

  def handle_entity(self, entity):
    """Performs processing on a single entity.
    
    Args:
      entity: A db.Model instance to update.
    """
    raise NotImplementedError()

  def finish(self, success, status):
    """Finish processing. Called after all entities have been updated.
    
    Args:
      success: boolean: Indicates if the process completed successfully, or was
        aborted due to too many errors.
      status: The model.Status object for this BulkUpdater.
    """
    pass

  def put(self, entities):
    """Stores updated entities to the datastore.
    
    Updates are batched for efficiency.
    
    Args:
      entities: An entity, or list of entities, to store.
    """
    if isinstance(entities, db.Model):
      entities = [entities]
    self.__to_put.extend(entities)
    self._status.num_put += len(entities)
    
    while len(self.__to_put) > self.PUT_BATCH_SIZE:
      db.put(self.__to_put[-self.PUT_BATCH_SIZE:])
      del self.__to_put[-self.PUT_BATCH_SIZE:]

  def delete(self, entities):
    """Deletes entities from the datastore.
    
    Deletes are batched for efficiency.
    
    Args:
      entities: An entity, key, or list of entities or keys, to delete.
    """
    if isinstance(entities, (db.Key, db.Model, basestring)):
      entities = [entities]
    self.__to_delete.extend(entities)
    self._status.num_deleted += len(entities)
    
    while len(self.__to_delete) > self.DELETE_BATCH_SIZE:
      db.delete(self.__to_delete[-self.DELETE_BATCH_SIZE:])
      del self.__to_delete[-self.DELETE_BATCH_SIZE:]

  def handle_exception(self):
    """Records a failed key. Internal."""
    self._status.num_errors += 1
    self.__log_entries.append(model.LogEntry(
        parent=self._status,
        task_id=self.task_id,
        log_key=self.current_key,
        is_error=True,
        message=''.join(traceback.format_exception(*sys.exc_info()))))

  def log(self, message):
    """Records a logging message."""
    self.__log_entries.append(model.LogEntry(
        parent=self._status,
        task_id=self.task_id,
        log_key=self.current_key,
        message=message))

  def __process_entities(self, q):
    """Processes a batch of entities.
    
    Args:
      q: A query to iterate over doing processing.
    Returns:
      True if the update process has finished, False otherwise.
    """
    end_time = time.time() + self.MAX_EXECUTION_TIME
    for entity in q:
      self.current_key = entity.key()
      try:
        self.handle_entity(entity)
      except (db.Timeout, apiproxy_errors.CapabilityDisabledError,
              apiproxy_errors.DeadlineExceededError):
        # Give up for now - reschedule for later.
        return False
      except Exception, e:
        # User exception - log and (perhaps) continue.
        logging.exception("Exception occurred while processing entity %r",
                          entity.key())
        self.handle_exception()
        if self.MAX_FAILURES >= 0:
          if self._status.num_errors > self.MAX_FAILURES:
            # Update completed (failure)
            self._status.state = model.Status.STATE_FAILED
            return True
      
      self._status.num_processed += 1
      
      if time.time() > end_time:
        return False
    
    # The loop finished - we're done!
    self._status.state = model.Status.STATE_COMPLETED
    return True

  def run(self, _start_cursor=None):
    """Begins or continues a batch update process."""
    status = self._status
    
    if not status:
      logging.error("Job entity not found.")
      return

    state_override_key = 'job_state:%s' % str(status.key())
    state_override = memcache.get(state_override_key, namespace='__bulkupdate')
    if state_override and status.state != state_override:
      status.state = state_override
      status.put()
      memcache.delete(state_override_key, namespace='__bulkupdate')

    if not status.is_running:
      logging.warn("Terminating cancelled job.")
      return

    q = self.get_query()
    if _start_cursor:
      q.with_cursor(_start_cursor)
    
    finished = self.__process_entities(q)
    
    self.current_key = None
    
    # Store or delete any remaining entities
    if self.__to_put:
      db.put(self.__to_put)
    if self.__to_delete:
      db.delete(self.__to_delete)

    log_entries = self.__log_entries
    self.__log_entries = []
        
    if finished:
      logging.info(
          "Processed %d entities in %d tasks, putting %d and deleting %d",
          status.num_processed, status.num_tasks, status.num_put,
          status.num_deleted)
      self.finish(status.state == model.Status.STATE_COMPLETED,
                  status)
    else:
      self.__to_put = []
      self.__to_delete = []
      self.task_id += 1
      self.current_key = None
      defer(self.run, q.cursor())

    status.num_tasks += 1
    status.last_update = datetime.datetime.now()
    log_entries.append(status)
    db.put(log_entries)

  def start(self):
    """Starts a BulkUpdater in a deferred task."""
    defer(self.run)
    return self._status.key()


class BulkPut(BulkUpdater):
  def __init__(self, query):
    super(BulkPut, self).__init__()
    self.query = query

  def get_query(self):
    return self.query

  def handle_entity(self, entity):
    self.put(entity)


class BulkDelete(BulkUpdater):
  def __init__(self, query):
    super(BulkDelete, self).__init__()
    self.query = query

  def get_query(self):
    return self.query

  def handle_entity(self, entity):
    self.delete(entity)
