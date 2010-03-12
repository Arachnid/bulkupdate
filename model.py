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
  return delta.days * 86400 + delta.seconds


def human_timedelta(ts):
  delta = datetime.datetime.now() - ts
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


class Status(db.Model):
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

  state = db.IntegerProperty(
      required=True,
      choices=STATE_NAMES.keys(),
      default=STATE_RUNNING)
  num_processed = db.IntegerProperty(required=True, default=0)
  num_errors = db.IntegerProperty(required=True, default=0)
  num_tasks = db.IntegerProperty(required=True, default=0)
  num_put = db.IntegerProperty(required=True, default=0)
  num_deleted = db.IntegerProperty(required=True, default=0)
  start_time = db.DateTimeProperty(required=True, auto_now_add=True)
  last_update = db.DateTimeProperty(required=True, auto_now_add=True)
  failed_keys = db.ListProperty(db.Key)
  messages = db.ListProperty(db.Text)

  @classmethod
  def kind(cls):
    return "__bulkupdate_%s" % (cls.__name__,)

  @property
  def log_entries(self):
    return LogEntry.all().ancestor(self)

  @property
  def state_name(self):
    return Status.STATE_NAMES[self.state]

  @property
  def is_running(self):
    return self.state == Status.STATE_RUNNING

  @property
  def is_deleting(self):
    return self.state == Status.STATE_DELETING

  start_time_delta = property(lambda self: human_timedelta(self.start_time))
  last_update_delta = property(lambda self: human_timedelta(self.last_update))

  def _calculate_rate(self, value):
    elapsed = float(timedelta_to_seconds(self.last_update - self.start_time)/60)
    if elapsed == 0:
      return "-"
    return "%.1f" % (value / elapsed,)

  processing_rate = property(lambda self: self._calculate_rate(self.num_processed))
  error_rate = property(lambda self: self._calculate_rate(self.num_errors))
  put_rate = property(lambda self: self._calculate_rate(self.num_put))
  delete_rate = property(lambda self: self._calculate_rate(self.num_deleted))
  task_rate = property(lambda self: self._calculate_rate(self.num_tasks))

  def delete(self):
    defer(_delete_job, self.key())
  

class LogEntry(db.Model):
  task_id = db.IntegerProperty(required=True)
  log_key = KeyProperty()
  is_error = db.BooleanProperty(required=True, default=False)
  message = db.TextProperty(required=True)
  timestamp = db.DateTimeProperty(required=True, auto_now_add=True)

  timestamp_delta = property(lambda self: human_timedelta(self.timestamp))

  @classmethod
  def kind(cls):
    return "__bulkupdate_%s" % (cls.__name__,)
