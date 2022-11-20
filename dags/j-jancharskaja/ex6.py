from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'j-jancharskaja_6',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'My fifth DAG',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 11, 11),
    catchup = False,
    tags = ['fifth']
) as dag:

    tasks = dict()

    for i in range(10):
        tasks[('t' + str(i))] = BashOperator(
            task_id = 'command_' + str(i),
            bash_command = 'echo $NUMBER',
            env = {'NUMBER': i}
        )


tasks['t0'] >> tasks['t1'] >> tasks['t2'] >> tasks['t3'] >> tasks['t4'] >> tasks['t5'] >> tasks['t6'] >> tasks['t7'] >> tasks['t8'] >> tasks['t9'] 