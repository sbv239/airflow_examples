from airflow import DAG

from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_d-trubitsin_6',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 23),
    catchup=False,
    tags=['d-trubitsin_3'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='bash_task_' + str(i),
            bash_command="echo $NUMBER",
            dag=dag,
            env={"NUMBER": i}
        )
