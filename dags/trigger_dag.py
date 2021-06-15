import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError



LOG_FORMAT = '%(asctime)s %(levelname)-10s %(name)-16s %(funcName)-20s <%(lineno)-3d> %(message)s'
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
TRIGGER_DIR = Variable.get('filepath', default_var='run.txt')

SLACK_TOKEN = Variable().get_variable_from_secrets(key="slack_secret")


def log_info(**kwargs):
    logger.info(kwargs['task_instance'].xcom_pull(key='msg'))


def get_subdag(start_date):
    with DAG('trigger_sensor.process_results_subdag', start_date=start_date) as dag:
        print_res = PythonOperator(task_id='logging',
                                   python_callable=log_info,
                                   provide_context=True)
        remove_op = BashOperator(task_id='remove_run_file',
                                 bash_command=f'rm {os.path.join(BASE_DIR, TRIGGER_DIR)}')
        create_ts = BashOperator(task_id='create_ts', bash_command="touch finished_{{ ts_nodash }}")
        print_res >> remove_op >> create_ts
    return dag


class SmartFileSensor(FileSensor):
    """ custom smart sensor """
    poke_context_fields = ('filepath', 'fs_conn_id')

    def __init__(self,  **kwargs):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):
        result = not self.soft_fail and super().is_smart_sensor_compatible()
        return result


with DAG('trigger_sensor', start_date=datetime(2021, 6, 10), schedule_interval="0 * * * *") as dag:
    f_sens = FileSensor(
        task_id='file_sensor',
        poke_interval=30,
        execution_timeout=timedelta(seconds=30),
        filepath=TRIGGER_DIR
    )
    trigger = TriggerDagRunOperator(
        task_id='trigger',
        trigger_dag_id='manual_dag_id_1'
    )
    call_sub = SubDagOperator(
        subdag=get_subdag(datetime(2021, 6, 10)),
        task_id='process_results_subdag'
    )
    f_sens >> trigger >> call_sub

with dag:
    """ main DAG:
    smart_sensor (looking for run file) ->
    trigger_external_dag (dag_id_DB_1) -> 
    SubDAG (external_sensor -> print_logs -> remove_file -> print_finish_log | example TaskGroup) -> 
    send_message (into Slack chanell)
    """
    @task()
    def slack_send_message():
        client = WebClient(token=SLACK_TOKEN)
        try:
            response = client.chat_postMessage(channel="airflowtask33", text="Hello from your app! :tada:")
        except SlackApiError as e:
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'


    sens = SmartFileSensor(task_id="checking_file", filepath=TRIGGER_DIR, fs_conn_id='fs_default')

    task_trigger = TriggerDagRunOperator(
        task_id="trigger_database_update", trigger_dag_id="dag_id_DB_1", wait_for_completion=True, poke_interval=15,
    )

    sub_dag = SubDagOperator(task_id='XCOM_sub_dag', subdag=get_subdag())

    task_slack = slack_send_message()

    sens >> task_trigger >> sub_dag >> task_slack
