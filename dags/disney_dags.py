# from airflow.models.dag import DAG
from src.dags.base.dag import BaseDAG
from src.modules.disney import *

class DisneyDAG(BaseDAG):
    default_tags = ['disney']

dags = [
    DisneyDAG(module = DisneyRaw, scheduler = '*/5 * * * *'),
    DisneyDAG(module = DisneyCurated, scheduler = '*/5 * * * *')
]

for dag in dags:
    globals()[dag.dag_name] = (dag.build())
