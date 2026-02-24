import logging


def notify_failure(context):
    ti = context['task_instance']
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = context.get('run_id')
    exception = context.get('exception')

    logging.error(
        f"""
        Task failed
        DAG: {dag_id}
        Task: {task_id}
        Run ID: {run_id}
        Error: {exception}
        """
    )


def notify_success(context):
    ti = context['task_instance']
    logging.info(f'Task {ti.task_id} in DAG {ti.dag_id} is run successfully')


def notify_retry(context):
    ti = context['task_instance']
    logging.warning(f'Task {ti.task_id} retrying (try {ti.retry_number})')
