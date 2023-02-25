from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'task_2_y-volkova-22',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
        description='task 2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['task2'],
) as dag:


    t1 = BashOperator(
        task_id='run_pwd',
        bash_command='pwd',
    )

    def my_func(ds):
        print(ds)

    t2 = PythonOperator(
        task_id='python_task',
        python_callable=my_func,
    )


    t1 >> t2
