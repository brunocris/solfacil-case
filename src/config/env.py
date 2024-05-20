from airflow.models.variable import Variable

# stage area dir
TMP_DIR = Variable.get('TMP_DIR')

# DAGS CUSTOM PARAMS
DAGS_CUSTOM_PARAMS = Variable.get('DAGS_CUSTOM_PARAMS', deserialize_json=True)
