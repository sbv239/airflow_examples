from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import requests
import json

url = 'https://covidtracking.com/api/v1/states/'
state = 'wa'


def get_testing_increase(state, ti):
    """
    Gets totalTestResultsIncrease field from Covid API for given state and returns value
    """
    res = requests.get(url + '{0}/current.json'.format(state))
    testing_increase = json.loads(res.text)['totalTestResultsIncrease']
    # в ti уходит task_instance, его передает Airflow под таким названием
    # когда вызывает функцию в ходе PythonOperator
    ti.xcom_push(
        key='testing_increase',
        value=testing_increase
    )


def task_2_python_script(ds):
    """
    Print ds
    """
    print(ds)


# Default settings applied to all tasks
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'xcom_dag',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    opr_d_nikolaev_20_1 = BashOperator(
        task_id='d-nikolaev-20_1',
        bash_command='pwd'

    )
    opr_d_nikolaev_20_2 = PythonOperator(
        task_id='d-nikolaev-20_2',
        python_callable=task_2_python_script,

    )

    opr_d_nikolaev_20_1 >> opr_d_nikolaev_20_2
