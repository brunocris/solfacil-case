import json, requests

from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

class SlackHook(LoggingMixin):

    conn_id = 'slack'

    def __init__(self):
        self.slack_conn = BaseHook.get_connection(self.conn_id)
        self.slack_webhook_url = f"{self.slack_conn.host}{self.slack_conn.schema}"
        self.slack_webhook_username = self.slack_conn.login

        self.slack_conn_extra = self.slack_conn.extra_dejson
        self.slack_alerts_channel = self.slack_conn_extra.get('alerts_channel', '#airflow-alerts')
        self.slack_icon_emoji = self.slack_conn_extra.get('icon_emoji', 'airflow')

    def _notify(self, message):

        payload = {
            "text": message,
            "channel": self.slack_alerts_channel,
            "username": self.slack_webhook_username,
            "icon_emoji": self.slack_icon_emoji
        }

        return requests.post(
            self.slack_webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )


    def info(self, dag, task, message, exec_date=datetime.now()-timedelta(hours=3)):
        slack_msg = f"""
            :information_source:  Task Info.

            *Dag*: `{dag}` 
            *Task*: `{task}`
            *Execution Time*: {exec_date.strftime('%Y-%m-%d %H:%M:%S')}
            *Info*: {message}
        """

        return self._notify(slack_msg)

    def error(self, dag, task, log_url, exec_date=datetime.now()-timedelta(hours=3)):
        slack_msg = f"""
            :double_red_exclamation_mark: Task Failed.

            *Dag*: `{dag}` 
            *Task*: `{task}`
            *Execution Time*: {exec_date.strftime('%Y-%m-%d %H:%M:%S')}
            *Log URL*: <{log_url}|link to acess log task>
        """

        return self._notify(slack_msg)