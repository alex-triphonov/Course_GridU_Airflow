import logging
from datetime import datetime, timedelta
from random import randint

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOG_FORMAT = '%(asctime)s %(levelname)-10s %(name)-16s %(funcName)-20s <%(lineno)-3d> %(message)s'
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

CONFIGS = {
    'manual_dag_id_1': {'schedule_interval': "0 * * * *", "start_date": datetime(2021, 6, 10)},
    'manual_dag_id_2': {'schedule_interval': "0 * * * *", "start_date": datetime(2021, 6, 10)},
    'manual_dag_id_3': {'schedule_interval': "0 * * * *", "start_date": datetime(2021, 6, 10)}
}
database = 'test_db'


def print_start(msg):
    logger.info(msg)


def check_table_exist(sql_to_get_schema, sql_to_check_table_exist, table_name):
    """callable to check if table exist """
    hook = PostgresHook(postgres_conn_id='postgres')
    query = hook.get_first(sql=sql_to_check_table_exist.format(table_name.lower()))

    if query:
        return "insert_row"
    else:
        return "create_table"


def insert_row(sql_query, table_name, custom_id, dt_now, **kwargs):
    """ postgres hook to insert a new row: | id | user | timestamp | """
    hook = PostgresHook(postgres_conn_id='postgres')
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(
        sql_query, (custom_id, kwargs["ti"].xcom_pull(task_ids="getting_current_user"), dt_now)
    )
    connection.commit()


class PGCountRows(BaseOperator):
    """ custom operator to insert a new row: | id | user | timestamp | """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        conn = PostgresHook(postgres_conn_id='postgres').get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {database};")
        count_r = cursor.fetchall()
        context["ti"].xcom_push(key=f"{database}_rows_count", value=count_r,)


for dag_id in CONFIGS:
    with DAG(dag_id, **CONFIGS[dag_id]) as dag:
        start = PythonOperator(
            task_id='print_start',
            python_callable=print_start,
            op_args=[f"{dag_id} start processing tables in database: {database}"],
        )

        bash = BashOperator(
            task_id='bash',
            bash_command='whoami',
        )

        task_check_exist = BranchPythonOperator(
            task_id="check_table_exist", python_callable=check_table_exist,
            op_args=["SELECT * FROM pg_tables;",
                     "SELECT * FROM information_schema.tables "
                     "WHERE table_name = '{}';", database])

        task_insert_row = PythonOperator(
            task_id='insert_row',
            python_callable=insert_row,
            op_args=[f"INSERT INTO {database} VALUES({database}, {randint(1, 10)}, {datetime.now()});"]
        )

        task_query = PGCountRows(task_id="query", trigger_rule=TriggerRule.NONE_FAILED)

        task_create_table = PostgresOperator(
            task_id='create_table',
            sql=f'''CREATE TABLE {database} (
                        custom_id integer NOT NULL, 
                        user_name VARCHAR (50) NOT NULL, 
                        timestamp TIMESTAMP NOT NULL
                        );''', trigger_rule=TriggerRule.NONE_FAILED,
        )

        start >> bash >> task_check_exist >> [task_create_table, task_insert_row] >> task_query
        globals()[dag_id] = dag
