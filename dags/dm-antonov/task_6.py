from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task_6_dm-antonov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='task_6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 4, 23),
        catchup=False,
        tags=['task_6']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='echo' + str(i),
            bash_command='echo $NUMBER',
            env={'NUMBER': i}
        )
