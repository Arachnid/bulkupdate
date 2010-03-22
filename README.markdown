App Engine Bulk Update Library
==============================

Introduction
------------
The App Engine bulk update library is a library for the App Engine Python
runtime, designed to facilitate doing bulk processing of datastore records. It
provides a framework for easily performing operations on all the entities
matched by any arbitrary datastore query, as well as providing an administrative
interface to monitor and control batch update jobs.


Installation
------------

Installation consists of 3 steps, one of which is optional.

### 1. Installing the library

Download a copy of the library, or check it out from git, and install it in a
directory named 'bulkupdate', directly off your application's root directory.

### 2. Installing the deferred handler

The bulkupdate library uses [deferred](http://code.google.com/appengine/articles/deferred.html)
to schedule and perform work on the task queue. If you do not already have the
deferred handler installed, you need to add it to your app.yaml. Add the
following block in the 'handlers' section of app.yaml:

    handlers:
      - url: /_ah/queue/deferred
        script: $PYTHON_LIB/google/appengine/ext/deferred/handler.py
        login: admin

Make sure you add this handler *before* any '.*' handler, or it will not be
matched.

### 3. Installing the admin interface (optional)

The final step is to install the optional admin interface, which lets you
monitor and cancel bulk update jobs. Add the following block in the 'handlers'
section of app.yaml:

    handlers:
      - url: /_ah/bulkupdate/admin/.*
        script: bulkupdate/handler.py
        login: admin

And add the following block in the admin_console section:

    admin_console:
      pages:
      - name: Bulk Update Jobs
        url: /_ah/bulkupdate/admin/


Usage
-----

Basic usage of the library consists of subclassing the bulkupdate.BulkUpdater
class, and overriding, at a minimum, the get_query() method, which is expected
to return a db.Query or db.GqlQuery object to process, and the handle_entity()
method, which is called once for each entity returned by the query.

To make modifications to the datastore, the handle_entity() method can call
self.put() and self.delete(), which batch update and delete operations for
optimal efficiency. Methods are not restricted to updating or deleting the
entity that was passed in, and may choose to insert, update, or delete any
record. Implementers intending to update related records, however, should bear
in mind that previous updates for that same record may still be waiting to be
written to the datastore. Future versions may address this by providing
alternatives to db.get() and standard queries that first check the local cache
for matching entities.

The bulkupdate library automatically splits the job into tasks, which are
executed serially on the task queue. As long as no single call to handle_entity
takes more than approximately 10 seconds, the automatic work splitting behaviour
will ensure tasks are scheduled properly and do not overrun available execution
time. Future versions of the library may implement parallelism to speed up
large updates.

Attributes stored against self _may_ be carried from one task to another. This
is done using the deferred module's default pickling behaviour. However, as the
order in which handle_entity is called is not defined, and because future
versions of the library may implement parallelization of updates, it is not
recommended that users rely on this behaviour.

As with any job that runs on the task queue, handle_entity implementations
should be made idempotent whenever possible - eg, they should handle being
called twice or more for the same entity in a single job gracefully.

Example Usage
-------------

Suppose you have a model defined like this:

    class DownloadableFile(db.Model):
      num_downloads_today = db.IntegerProperty(required=True, default=0)
      last_reset = db.DateTimeProperty(required=True, auto_now_add=True)
      # Other properties as appropriate.

Once per day, you want to reset the num_downloads_today count to zero. You could
write a BulkUpdater subclass to do this as follows:

    class DownloadCountResetter(bulkupdate.BulkUpdater):
      DELETE_COMPLETED_JOB_DELAY = 0
      DELETE_FAILED_JOBS = False

      UPDATE_INTERVAL = datetime.timedelta(days=1)

      def get_query(self):
        return DownloadableFile.all().order('last_reset')

      def handle_entity(self, entity):
        if (datetime.datetime.now() - entity.last_reset) > self.UPDATE_INTERVAL:
          entity.num_downloads_today = 0
          entity.last_reset = datetime.datetime.now()
          self.put(entity)

Because we don't really care about logging information and statistics for
successful update jobs, we set the DELETE_COMPLETED_JOB_DELAY to 0. We do care
about failed jobs, though, so we set DELETE_FAILED_JOBS to False, ensuring
that information on failed jobs is never automatically deleted.

Note that in get_query, we order the query by last_reset. Although this may not
be respected entirely in future versions of the bulkupdate library, here it
helps to ensure that the most out of date records are reset first.

Finally, the handle_entity method checks each entity to see if it's been at
least 1 day since it was last reset, and if it has, resets it again. This check
is necessary so that if a task gets re-executed, we don't reset the download
count for an entity multiple times in a day. Also note that we use self.put()
instead of db.put(), which ensures updates are batched efficiently.

To run this bulk update job, you would do something akin to the following from
within a scheduled task that runs once a day:

    job = DownloadCountResetter()
    job.start()


Convenience Classes
-------------------

The bulkupdate library comes with a couple of subclasses of BulkUpdater that
automate common operations: BulkPut and BulkDelete.

BulkPut takes a query in its constructor, and fetches and re-puts every entity
matching that query. This is useful for re-storing entities whose model
definitions have changed, or for eliminating indexing bugs.

Example usage:

    job = bulkupdate.BulkPut(DownloadableFile.all())
    job.start()

BulkDelete takes a query in its constructor, and deletes every entity matching
that query. This is useful for cleaning up out of date or unwanted records
en-masse.

Example usage:

    job = bulkupdate.BulkDelete(DownloadableFile.all(keys_only=True))
    job.start()

Reference
---------

### Class Attributes

A number of class attributes are available to customize the behaviour of batch
update jobs. These may be set in the class definition of subclasses, or
overridden in __init__ so as to affect the behaviour of only a single instance.

*PUT_BATCH_SIZE* - Default 20. The size of batches to accumulate before making a
put() operation against the datastore. Even if a single handle_entity call
attempts to put a batch larger than this, it will be automatically broken up
into batches no larger than this value. You may want to decrease this if your
entities are particularly large or complex, or increase it if they are small and
simple. See the note below on the interaction of this value with
MAX_EXECUTION_TIME.

*DELETE_BATCH_SIZE* - Default 100. The size of batches to accumulate before
making a delete() operation against the datastore. Even if a single
handle_entity call attempts to delete a batch larger than this, it will be
automatically broken up into batches no larger than this value. You may want to
decrease this if your entities have many index entries. See the note below on
the interaction of this value with MAX_EXECUTION_TIME.

*MAX_EXECUTION_TIME* - Default 20.0. The maximum number of seconds a task should
process entities for before enqueueing the next task and returning. Execution
stops after the first handle_entity call that returns after this long executing
and the put and delete batches have been flushed to the datastore. See the note
below on the interaction of this value with PUT_BATCH_SIZE and DELETE_BATCH_SIZE.

*MAX_FAILURES* - Default 0. Maximum number of failures to tolerate before
aborting the bulk update job. A failure is defined as an uncaught exception
thrown by the handle_entity method. Setting this to -1 prevents the job from
aborting no matter how many exceptions are encountered.

*DELETE_COMPLETED_JOB_DELAY* - Default 24h (60 * 60 * 24). The delay between
a job completing or failing, and its information records being deleted. Setting
this to 0 causes the records to be deleted immediately on completion, while
setting it to -1 prevents job information records from being deleted at all.

*DELETE_FAILED_JOBS* - Default True. If True, all job records are deleted after
DELETE_COMPLETED_JOB_DELAY. If False, only successful job records are deleted.

### Methods

#### get_query()

Must be overridden by subclasses. Returns a db.Query or db.GqlQuery object that
represents the query to iterate over. The query returned must be the same for
every invocation of this method. The query returned must be capable of returning
a cursor - eg, it may not use the IN or != operators. Otherwise, there are no
restrictions on the nature of the query, though ordering clauses may be ignored.

#### handle_entity(entity)

Must be overridden by subclasses. Called for each entity to be processed, with
the entity in question passed in. If the query returned by get_query() is a
keys_only query, the argument to this method is a db.Key, rather than an entity.

To make updates to entities, subclasses should call the put() method on this
class. Likewise, to delete entities, they should call the delete() method on
this class. These methods batch updates to improve efficiency over calling
db.put() or db.delete() directly.

#### finish(success, status)

Called by the final task when an update completes. success is a boolean
which will be True iff the bulkupdate job succeeded, while status is a
bulkupdate.model.Status entity containing status information about the completed
job. May be left unimplemented if no special completion behaviour is desired.

#### put(entities)

Queues one or more entities for writing to the datastore. The argument may be
either a single entity, or a list of entities. Updates are written to the
datastore whenever the number of queued entities is at least PUT_BATCH_SIZE.

#### delete(entities)

Queues one or more entities for deletion. The argument may be either a single
entity or key, or a list of either. Updates are written to the datastore
whenever the number of queued keys is at least DELETE_BATCH_SIZE.

#### log(message)

Writes a message to the bulkupdate task's log. Log messages show up in the admin
console page for this job, and are also accessible programmatically.

#### start(**kwargs)

Enqueues a deferred task to start this bulkupdate job.
