""" add additional DAGs folders """
import os
from airflow.models import DagBag

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

dags_dirs = [f'{BASE_DIR}/dags', f'{BASE_DIR}/dags2', f'{BASE_DIR}/dags3']

for dr in dags_dirs:
    dag_bag = DagBag(dr)

if dag_bag:
    for dag_id, dag in dag_bag.dags.items():
        globals()[dag_id] = dag
