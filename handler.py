from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app

import os

from bulkupdate import model


class BaseHandler(webapp.RequestHandler):
  def render_template(self, name, template_args):
    path = os.path.join(os.path.dirname(__file__), 'templates', name)
    self.response.out.write(template.render(path, template_args))


class JobListingHandler(BaseHandler):
  def get(self):
    running_jobs = []
    old_jobs = []
    for job in model.Status.all().order('state').fetch(50):
      if job.state == model.Status.STATE_RUNNING:
        running_jobs.append(job)
      else:
        old_jobs.append(job)
    self.render_template('jobs.html', {
        'base_url': self.request.url,
        'running': running_jobs,
        'completed': old_jobs,
    })


class JobStatusHandler(BaseHandler):
  def get_job(self):
    id = self.request.GET.get('id')
    if not id or not id.isnumeric():
      self.error(400)
      return
    job = model.Status.get_by_id(int(id))
    if not job:
      self.error(404)
      return
    return job
  
  def get(self):
    job = self.get_job()
    if not job: return
    
    self.render_template('status.html', {
        'job': job,
        'base_url': self.request.url,
    })

  def post(self):
    continue_url = self.request.GET.get('continue', self.request.url)
    new_state = int(self.request.POST['state'])

    def _tx():
      job = self.get_job()
      if not job: return False

      if (job.state == model.Status.STATE_RUNNING
          and new_state == model.Status.STATE_CANCELLED):
        job.state = model.Status.STATE_CANCELLED
        # Set memcache key to ensure job really gets cancelled
        memcache.set('job_state:%s' % str(job.key()),
                     model.Status.STATE_CANCELLED, namespace='__bulkupdate')
      elif (job.state != model.Status.STATE_RUNNING
            and new_state == model.Status.STATE_DELETING):
        memcache.set('job_state:%s' % str(job.key()),
                     model.Status.STATE_DELETING, namespace='__bulkupdate')
        job.state = model.Status.STATE_DELETING
        job.delete()
      else:
        return False
      job.put()
      return True

    if db.run_in_transaction(_tx):
      self.redirect(continue_url)
    else:
      self.error(400)


application = webapp.WSGIApplication([
  ('.*/', JobListingHandler),
  ('.*/status', JobStatusHandler),
], debug=os.environ['SERVER_SOFTWARE'].startswith('Dev'))


def main():
  run_wsgi_app(application)


if __name__ == '__main__':
  main()
