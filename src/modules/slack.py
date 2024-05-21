from hooks.slack_hook import SlackHook

def task_fail_slack_alert(context):

    task_instance = context.get('task_instance')
    
    return SlackHook().error(
        dag=task_instance.dag_id,
        task=task_instance.task_id,
        log_url=task_instance.log_url,
        exec_date=task_instance.execution_date
    )
