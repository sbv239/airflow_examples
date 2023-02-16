from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_6_a-tjurin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='Task_6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 16),
        catchup=False,

        tags=['Task_6'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_in_bash_env_' + str(i),
            env = {'NUMBER': str(i)},
            bash_command=f"echo $NUMBER",
        )


    t1
