from airflow.models import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from src.modules.base.module import BaseModule

from datetime import datetime, timedelta
from src.modules.slack import task_fail_slack_alert
from src.config.env import DAGS_CUSTOM_PARAMS


class BaseDAG(DAG):

    """

        The BaseDAG is a basic class model for DAGs that abstracts common features when we're working with Airflow.

        This class is based on airflow's default DAG class (source code: https://github.com/apache/airflow/blob/main/airflow/models/dag.py#L350)
        and has a couple of optimizations built in, like flags and custom params.

        Args:

            - module (BaseModule): a class that inherits from BaseModule. This is the class that implements the
                      actual code that runs inside of the databases and transformations.

            - scheduler (str): a cron schedule code following Airflow's model:
                         'minute hour day-of-month month day-of-week'

            - limit (int): maximum quantity of rows that can be extracted from the database.

            - chunk_size (int): maxium quantity of rows that can be loaded to the data lake with each run

        Kwargs:

            - dag_name (str): name of the dag, this is normally implemented inside of the module, based in a
                              standard, but sometimes we may want to use another name, in those cases, you must pass
                              it here.

            - timeout_hours (int): quantity of hours that the DAG fails after running continuously, this prevents a
                                   DAG from running indefinitely. By default, we set a timeout of 1 hour, if you need 
                                   to use another delta (if you know, for example, that a DAG takes more than an hour)
                                   you pass it in this parameter. To block timeout, use the "no-timeout"flag.
                                   
        """

    default_tags:list = []

    def __init__(self, module:BaseModule, scheduler:str = '@once',
                 limit:int = 10_000, chunk_size:int = 20_000, **kwargs):

        self.module = module()
        self.dag_name = kwargs.get("dag_name") if kwargs.get("dag_name") else self.module.dag_name
        self.timeout = timedelta(hours= kwargs.get("timout_hours")) if kwargs.get("timout_hours") else timedelta(hours=1)

        self.scheduler, self.limit, self.chunk_size, aditional_tags, flags = self._get_variables(scheduler, limit, chunk_size)
        self.tags = self.default_tags + aditional_tags

        alert_function, timeout = self._parse_flags(flags)
        
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'on_failure_callback': alert_function,
            'retries': 0
        }

        super().__init__(dag_id = self.dag_name, default_args = default_args, 
                         max_active_runs = 1, schedule_interval = self.scheduler, 
                         catchup = False, tags = self.tags, 
                         dagrun_timeout = timeout)

    def build(self):
        """

        The `build` method is used to define the tasks to be executed and in which order.
        The default process is:

        start > extract > load > delete_temp_files > end

        It should be noted that, to use this BaseDAG with the dafault build sequence, the base module
        must use the default names: `extract`, `load` and `delete_temp_file`. Otherwise, the method
        must be overwritten.

        """

        with self:

            start_task = DummyOperator(
                task_id = 'start_task'
            )

            extract_task = PythonOperator(
                task_id = 'extract_task',
                python_callable = self.module.extract
            )
            
            load_task = PythonOperator(
                task_id='load_task',
                python_callable=self.module.load
            )

            delete_temp_file = PythonOperator(
                task_id='delete_temp_file_task',
                python_callable=self.module.delete_temp_file,
            )

            end_task = DummyOperator(
                task_id = 'end_task'
            )

            start_task >> extract_task >> load_task >> delete_temp_file >> end_task

        return self

    def _get_variables(self, default_scheduler, default_limit, default_chunk_size):
        """

        Inside of Airflow's evironment variables, there is a variable called
        DAGS_CUSTOM_PARAMS which is used to overwrite certains aspects of DAGs, such as
        the normal scheduler or limit.

        The possible parameters are:

        - scheduler (str): the DAG's cron schedule.
        - limit (int): size of the batches to be extracted from the database.
        - chunksize (int): size of the batches to be inserted to the datalake.
        - tags (list): list of tags for the DAG.
        - flags (list): list of modifying flags, all possibilities are presented on the 
                        `_parse_flags` method.
        
        """

        dag_custom_params_env = DAGS_CUSTOM_PARAMS.get(self.dag_name, {})

        scheduler = dag_custom_params_env.get('scheduler', default_scheduler)
        limit = dag_custom_params_env.get('limit', default_limit)
        chunk_size = dag_custom_params_env.get('chunk_size', default_chunk_size)
        tags = dag_custom_params_env.get('tags', [])
        flags = dag_custom_params_env.get('flags', [])

        return scheduler, limit, chunk_size, tags, flags
    
    def _parse_flags(self, flags_lst):
        """

        This method parses the flags that can be passed through the CUSTOM PARAMS variable.
        Flags are a way to easily modify the behavior of the DAG. Currently, the flags are:

        - bypass-alert:  make it so that error during a DAG's execution doesn't generate a
                         message on #data-alerts channel. This is useful for tests or development.

        - no-timeout:    make it so that the DAG doesn't have a timeout, meaning that it doesn't
                         automatically breaks if enough time has passed. Default parameter is 1 hour
                         timeout.

        - alert-success: generates an alert if the DAG has successfully ran. *NOT YET IMPLEMENTED*

        """

        if 'bypass-alert' in flags_lst:
            function = self._bypass_alert
        else:
            function = task_fail_slack_alert
        
        if 'no-timeout' in flags_lst:
            timeout = None
        else:
            timeout = self.timeout
        
        return function, timeout


    def _bypass_alert(self):
        """

        Dummy method for bypassing slack alerts.
        
        """
        
        self.log.info("The dag was configured to bypass Slack Alerts.")