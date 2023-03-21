from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('task2NN',
         default_args={
             'depends_on_past': False,
             'email': ['airflow@example.com'],
             'email_on_failure': False,
             'email_on_retry': False,
             'retries': 1,
             'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
         },
         description='My first DAG',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 3, 21),
         catchup=True,
         tags=['NNDAG2'],
         ) as dag:
    task1 = BashOperator(
        task_id='BashPWD',
        bash_command='pwd'
    )


    def print_something(ds):
        return print(ds)


    task2 = PythonOperator(
        task_id='Pyprint',
        python_callable=print_something
    )

    task1 >> task2
