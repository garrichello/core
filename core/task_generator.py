from celery.contrib import rdb

def task_generator(json_task, task_id):
    print(json_task)
    rdb.set_trace()

    return 0

