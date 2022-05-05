from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    "hw_5_v-baev-7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Env in BashOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=5, day=3),
    catchup=False,
    tags=['hw_5'],
) as dag:
        for i in range(10):
                task_b = BashOperator(
                task_id='echo'+str(i),
                bash_command="echo $NUMBER",
                env = {"NUMBER": i}
        )
task_b