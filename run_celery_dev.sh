virtualenv/bin/celery -A core worker --loglevel=INFO -E -n debugworker1@%h -Q starter_queue_dev,workers_queue_dev

