#    Copyright 2013 10gen Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from apscheduler.scheduler import Scheduler
from mws.db import get_db
from datetime import datetime, timedelta
from mws.util import get_internal_coll_name

EXPIRE_SESSION_EVERY = 600
EXPIRE_SESSION_DURATION = 1800


def run_scheduler(app):
    scheduler = Scheduler()

    expire_wrapper = lambda: expire_sessions(app)
    scheduler.add_interval_job(expire_wrapper, seconds=EXPIRE_SESSION_EVERY)

    scheduler.start()
    print "APScheduler started successfully"


def expire_sessions(app):
    with app.app_context():
        db = get_db(MWSExceptions=False)
        delta = timedelta(seconds=EXPIRE_SESSION_DURATION)
        exp = datetime.now() - delta
        sessions = db.clients.find({'timestamp': {'$lt': exp}})
        for sess in sessions:
            db.clients.remove(sess)
            # Todo: Only remove collections if no one else is using this res_id
            res_id = sess['res_id']
            for c in sess['collections']:
                db.drop_collection(get_internal_coll_name(res_id, c))
        app.logger.info('Timed out expired sessions dead before %s' % exp)
