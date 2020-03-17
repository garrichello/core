from kombu import Exchange, Queue

## Broker settings.
broker_url = 'amqp://admin:adminpass@abak.scert.ru:5672/core'

# List of modules to import when the Celery worker starts.
imports = ('core',)

## Using rpc to return task state and results.
result_backend = 'rpc://'

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
task_create_missing_queues = False
task_queues = (
    Queue('plain_xml_queue', Exchange('default'), routing_key='plain_xml_queue'),
    Queue('json_task_queue', Exchange('default'), routing_key='json_task_queue'),
    Queue('rpc_queue', Exchange('default'), routing_key='rpc_queue'),
)

# Define routes
task_routes = {
    'core.tasks.run_json_task': {
        'queue': 'json_task_queue',
        'exchange': 'default',
        'routing_key': 'json_task_queue'
    },
    'core.tasks.run_plain_xml': {
        'queue': 'plain_xml_queue',
        'exchange': 'default',
        'routing_key': 'plain_xml_queue'
    }
}

# Default input queue
#task_default_queue = 'json_task_queue'
#task_default_exchange = 'default'
#task_default_routing_key = 'json_task_queue'

worker_redirect_stdouts_level = 'INFO'
