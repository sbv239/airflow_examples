from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'a-buzmakov-13_task_3',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='a-buzmakov-13_DAG_task3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_3'],
) as dag:
    def get_task_number(ts,run_id,**kwargs):
        print(ts)
        print(run_id)
    for i in range(20):
        a=PythonOperator(
            task_id='task_number'+str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number':i})
