from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

from airflow import DAG

import requests
import json

url = 'https://covidtracking.com/api/v1/states/'
state = 'wa'

def get_testing_increase(state, ti):
    """
    Gets totalTestResultsIncrease field from Covid API for given state and returns value
    """
    res = requests.get(url+'{0}/current.json'.format(state))
    testing_increase = json.loads(res.text)['totalTestResultsIncrease']

    ti.xcom_push(key='testing_increase', value=testing_increase)

def analyze_testing_increases(state, ti):
    """
    Evaluates testing increase results
    """
    testing_increases=ti.xcom_pull(key='testing_increase', task_ids='get_testing_increase_data_{0}'.format(state))
    print('Testing increases for {0}:'.format(state), testing_increases)
    #run some analysis here

def print_context(ts, run_id, **kwargs):
    """Пример PythonOperator"""
    # Через синтаксис **kwargs можно получить словарь
    # с настройками Airflow. Значения оттуда могут пригодиться.
    # Пока нам не нужно
    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    print(ts)
    print(run_id)
    print(f'task number is {kwargs["task_number"]}')
    return 'Whatever you return gets printed in the logs'

def get_testing_increase(state, ti):
    res = requests.get()


with DAG(
    'hw_8_de-jakovlev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo $NUMBER
    {% endfor %}
        """
    )
    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i}',
            bash_command=templated_command,
            env={"NUMBER": f'{i}'}
        )
    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_the_data_{i}',
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )

    opr_get_covid_data = PythonOperator(
        task_id='get_testing_increase_data_{0}'.format(state),
        python_callable=get_testing_increase,
        op_kwargs={'state': state}
    )

    opr_analyze_testing_data = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_testing_increases,
        op_kwargs={'state': state}
    )

    opr_get_covid_data >> opr_analyze_testing_data








