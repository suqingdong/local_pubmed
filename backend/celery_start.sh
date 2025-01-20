celery -A backend worker -l info --logfile celery_worker.log --pidfile celery_worker.pid --beat 

# celery -A backend beat -l info --logfile celery_beat.log --pidfile celery_beat.pid
