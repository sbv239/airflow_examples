from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'task_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_2 dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 21),
        catchup=False,
        tags=['task_2'],
) as dag:
    task_1 = BashOperator(
        task_id="bash pwd",
        bash_command="pwd"
    )

    def print_ds(ds):
        print(ds)
        print("ds printed")

    task_2 = PythonOperator(
        task_id="print_ds",
        python_callable=print_ds
    )

    task_1 >> task_2
