import datetime

from google.appengine.ext import db
from google.appengine.ext.deferred import defer


class KeyProperty(db.Property):
  """A property that stores a key, without automatically dereferencing it.
 
  Example usage:
 
  >>> class SampleModel(db.Model):
  ...   sample_key = KeyProperty()
 
  >>> model = SampleModel()
  >>> model.sample_key = db.Key.from_path("Foo", "bar")
  >>> model.put() # doctest: +ELLIPSIS
  datastore_types.Key.from_path(u'SampleModel', ...)
 
  >>> model.sample_key # doctest: +ELLIPSIS
  datastore_types.Key.from_path(u'Foo', u'bar', ...)
  """
  def validate(self, value):
    """Validate the value.
 
    Args:
      value: The value to validate.
    Returns:
      A valid key.
    """
    if isinstance(value, basestring):
      value = db.Key(value)
    if value is not None:
      if not isinstance(value, db.Key):
        raise TypeError("Property %s must be an instance of db.Key"
                        % (self.name,))
    return super(KeyProperty, self).validate(value)


def timedelta_to_seconds(delta):
  """Converts a datetime.timedelta into a number of seconds."""
  return delta.days * 86400.0 + delta.seconds


def human_timedelta(ts):
  """Converts a timedelta into a human readable description of time elapsed.
  
  If the input is a datetime, it is subtracted from the current datetime.
  """
  if isinstance(ts, datetime.datetime):
    delta = datetime.datetime.now() - ts
  else:
    delta = ts
  elapsed = float(timedelta_to_seconds(delta))
  if elapsed <= 90:
    return "%d seconds" % (elapsed,)
  elapsed /= 60
  if elapsed <= 90:
    return "%d minutes" % (elapsed,)
  elapsed /= 60
  if elapsed <= 48:
    return "%d hours" % (elapsed,)
  elapsed /= 24
  return "%d days" % (elapsed,)


def _delete_job(key):
  """Deletes records for a job."""
  # Delete all child entities
  cursor = None
  while True:
    q = db.Query(keys_only=True).ancestor(key)
    if cursor:
      q.with_cursor(cursor)
    delete_keys = q.fetch(500)
    if not delete_keys:
      break
    db.delete(delete_keys)
    cursor = q.cursor()

  # Delete the entity itself
  db.delete(key)


def rate_property(rise_prop, run_prop):
  """Creates custom properties that calculate the rate of change."""
  def ratefunc(self):
    rise = rise_prop.__get__(self, type(self))
    run = run_prop.__get__(self, type(self))
    if not run:
      return '-'
    else:
      return '%.1f' % (rise/float(run),)
  return property(ratefunc)


class Status(db.Model):
  """Encapsulates status information about a bulkupdate job."""

  STATE_RUNNING = 1L
  STATE_FAILED = 2L
  STATE_CANCELLED = 3L
  STATE_COMPLETED = 4L
  STATE_DELETING = 5L

  STATE_NAMES = {
    STATE_RUNNING: 'Running',
    STATE_FAILED: 'Failed',
    STATE_CANCELLED: 'Cancelled',
    STATE_COMPLETED: 'Completed',
    STATE_DELETING: 'Deleting',
  }

  # The current state
  state = db.IntegerProperty(
      required=True,
      choices=STATE_NAMES.keys(),
      default=STATE_RUNNING)

  # Number of entities processed
  num_processed = db.IntegerProperty(required=True, default=0)
  # Number of entities that caused errors
  num_errors = db.IntegerProperty(required=True, default=0)
  # Number of tasks executed so far
  num_tasks = db.IntegerProperty(required=True, default=0)
  # Number of entities written
  num_put = db.IntegerProperty(required=True, default=0)
  # Number of entities deleted
  num_deleted = db.IntegerProperty(required=True, default=0)
  # Datetime that this process started
  start_time = db.DateTimeProperty(required=True, auto_now_add=True)
  # Last time this status was updated
  last_update = db.DateTimeProperty(required=True, auto_now_add=True)

  @classmethod
  def kind(cls):
    return "__bulkupdate_%s" % (cls.__name__,)

  @property
  def log_entries(self):
    """Returns a query that fetches log entries."""
    return LogEntry.all().ancestor(self)

  @property
  def state_name(self):
    """Returns the human readable name of the current state."""
    return Status.STATE_NAMES[self.state]

  @property
  def is_running(self):
    """Returns true if this update is still running."""
    return self.state == Status.STATE_RUNNING

  @property
  def is_deleting(self):
    """Returns true if this update is finished and being deleted."""
    return self.state == Status.STATE_DELETING

  # Total time elapsed between start time and last update.
  elapsed_time = property(lambda self: self.last_update - self.start_time)
  # Human readable delta since start time
  start_time_delta = property(lambda self: human_timedelta(self.start_time))
  # Human readable delta since last update
  last_update_delta = property(lambda self: human_timedelta(self.last_update))
  # Human readable runtime
  total_runtime = property(lambda self: human_timedelta(self.elapsed_time))

  # Total time elapsed in seconds
  elapsed_seconds = property(
      lambda self: timedelta_to_seconds(self.elapsed_time))
  # Entities processed per second
  processing_rate = rate_property(num_processed, elapsed_seconds)
  # Errors encountered per second
  error_rate = rate_property(num_errors, elapsed_seconds)
  # Entities put per second
  put_rate = rate_property(num_put, elapsed_seconds)
  # Entities deleted per second
  delete_rate = rate_property(num_deleted, elapsed_seconds)
  # Tasks executed per second
  task_rate = rate_property(num_tasks, elapsed_seconds)

  # Entities processed per task
  task_processing_rate = rate_property(num_processed, num_tasks)
  # Errors per task
  task_error_rate = rate_property(num_errors, num_tasks)
  # Entities put per task
  task_put_rate = rate_property(num_put, num_tasks)
  # Entities deleted per task
  task_delete_rate = rate_property(num_deleted, num_tasks)
  # Time taken per task
  task_time = rate_property(elapsed_seconds, num_tasks)

  def delete(self, **kwargs):
    """Deletes information about this (completed) bulkupdate.
    
    Args:
      _countdown: Optional, how far in the future to begin deletion.
      _eta: Optional, date and time to delete info at.
    """
    defer(_delete_job, self.key(), **kwargs)
  

class LogEntry(db.Model):
  """Encapsulates a single log entry for a bulkupdate task."""

  # The number of the task that generated the log entry
  task_id = db.IntegerProperty(required=True)
  # The key of the entity that generated the log entry
  log_key = KeyProperty()
  # Is this log entry an error?
  is_error = db.BooleanProperty(required=True, default=False)
  # The log message, or the exception stacktrace if this is an error.
  message = db.TextProperty(required=True)
  # The datetime at which this message was logged
  timestamp = db.DateTimeProperty(required=True, auto_now_add=True)

  # Elapsed time since this message was logged
  timestamp_delta = property(lambda self: human_timedelta(self.timestamp))

  @classmethod
  def kind(cls):
    return "__bulkupdate_%s" % (cls.__name__,)
