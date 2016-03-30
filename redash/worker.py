import sys
from random import randint
from celery import Celery
from datetime import timedelta
from celery.schedules import crontab
from celery.signals import task_failure

from redash import settings, __version__

celery = Celery('redash',
                broker=settings.CELERY_BROKER,
                include='redash.tasks')

celery_schedule = {
    'refresh_queries': {
        'task': 'redash.tasks.refresh_queries',
        'schedule': timedelta(seconds=30)
    },
    'cleanup_tasks': {
        'task': 'redash.tasks.cleanup_tasks',
        'schedule': timedelta(minutes=5)
    },
    'refresh_schemas': {
        'task': 'redash.tasks.refresh_schemas',
        'schedule': timedelta(minutes=30)
    }
}

if settings.VERSION_CHECK:
    celery_schedule['version_check'] = {
        'task': 'redash.tasks.version_check',
        # We need to schedule the version check to run at a random hour/minute, to spread the requests from all users
        # evenly.
        'schedule': crontab(minute=randint(0, 59), hour=randint(0, 23))
    }

if settings.QUERY_RESULTS_CLEANUP_ENABLED:
    celery_schedule['cleanup_query_results'] = {
        'task': 'redash.tasks.cleanup_query_results',
        'schedule': timedelta(minutes=5)
    }

celery.conf.update(CELERY_RESULT_BACKEND=settings.CELERY_BACKEND,
                   CELERYBEAT_SCHEDULE=celery_schedule,
                   CELERY_TIMEZONE='UTC')

if settings.SENTRY_DSN:
    from raven import Client

    def process_failure_signal(sender, task_id, args, kwargs, **kw):
        exc_info = sys.exc_info()

        # Use name to avoid importing QueryExecutionError (results in cyclic imports):
        # TODO: move all custom exceptions into a separate module for this reason.
        if exc_info[1].__class__.__name__ == 'QueryExecutionError':
            return

        # This signal is fired inside the stack so let raven do its magic
        client.captureException(
                extra={
                    'task_id': task_id,
                    'task': sender,
                    'args': args,
                    'kwargs': kwargs,
                })

    client = Client(settings.SENTRY_DSN, release=__version__)
    task_failure.connect(process_failure_signal, weak=False)


