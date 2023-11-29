from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_g-hristov_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:
    for i in range(10):
        NUMBER = i
        taskbo = BashOperator(
            task_id='hw_g-hristov_6_BO'+str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": NUMBER}
        )
