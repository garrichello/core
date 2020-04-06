from kombu import Exchange, Queue

## Broker settings.
broker_url = 'amqp://admin:adminpass@abak.scert.ru:5672/core'

# List of modules to import when the Celery worker starts.
imports = ('core',)

## Using rpc to return task state and results.
#result_backend = 'rpc://'
result_backend = 'redis://abak.scert.ru/0'

result_persistent = False

task_serializer = 'json'

result_serializer = 'json'

accept_content = ['json']

timezone = 'Asia/Novosibirsk'

enable_utc = True

# No more than 10 tasks per second!
task_annotations = {'*': {'rate_limit': '10/s'}}

# Track start of a task
task_track_started = True

# Define queues
task_create_missing_queues = True
task_queues = (
    Queue('workers_queue', Exchange('default'), routing_key='worker.abak.scert.ru'),
    Queue('starter_queue', Exchange('default'), routing_key='starter.abak.scert.ru'),
)

# Define routes
task_routes = {
    'core.tasks.starter': {
        'queue': 'starter_queue',
        'exchange': 'default',
        'routing_key': 'starter.abak.scert.ru'
    },
    'core.tasks.worker': {
        'queue': 'workers_queue',
        'exchange': 'default',
        'routing_key': 'worker.abak.scert.ru'
    }
}

worker_redirect_stdouts_level = 'INFO'
