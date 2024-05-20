import requests, time, pandas as pd

from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException

class DisneyApiHook:
    """
    Creates a Disney API hook to make requests
    Base URL to request: https://api.disneyapi.dev
    Access the public API doc at https://disneyapi.dev/docs/
    """
    
    # Deafult http method is defined because this API is GET only.
    def __init__(self, method = 'GET'):
        self.hook = HttpHook(method = method, http_conn_id = 'slack_api')
        self.conn = self.hook.get_connection(self.hook.http_conn_id)

    def _request_data(self, endpoint, param = None):
        
        base_url = str(self.conn.host)

        if (base_url and not base_url.endswith('/')) and (endpoint and not endpoint.startswith('/')):
            url = base_url + '/' + endpoint
        else:
            url = (base_url or '') + (endpoint or '')
            
        if param:
            response = requests.get(url, params = param)
        else:
            response = requests.get(url)

        if not response.ok:
            raise AirflowException(
                f'Failed to request API: {response.status_code} | {response.text}')
        else:
            return response.json()
        
    def extract(self):
        response = self._request_data('character')
        print(response)