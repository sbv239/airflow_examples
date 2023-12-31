
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime 
with DAG(
    'hw_a-hlopov-25_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 10, 25)
) as dag:
    t1 = BashOperator(
        task_id="BashPwd",
        bash_command="pwd",  
    )
    def print_date(ds):
        print(ds)
        print("Date printed")
    t2 = PythonOperator(
        task_id='PrintDs',
        python_callable=print_date
    )

    t1>>t2


