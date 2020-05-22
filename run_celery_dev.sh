virtualenv/bin/celery worker --logfile=logs/%n%I.log --loglevel=INFO -E -n debugworker@%h -c 4 --config celeryconfig_dev

