from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def data_push ():
    return "Airflow tracks everything"

def data_pull(ti):
    return ti.xcom_pull(
        key = "return_value",
        task_ids = 'data_pushing'
    )


with DAG ('hw_pave-sokolov_9',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
            },
            description= 'HW 9 DAG',
            schedule_interval=timedelta(days = 1),
            start_date= datetime(2023,6,12),
            catchup= False,
            tags= ['example']
        ) as dag:
    
    task_push = PythonOperator(
        task_id = 'data_pushing',
        python_callable= data_push
    )

    task_pull = PythonOperator(
        task_id = 'data_pulling',
        python_callable= data_pull
    )

    task_push >> task_pull