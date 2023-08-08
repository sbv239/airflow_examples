"""
simple dag
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'Tima_first_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  #
    },

    description='1_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 6),
    catchup=False,
    tags=['tima'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = f'task_number_is{i}',
            bash_command = f'"echo {i}"'
    )


    def my_sleeping_function(random_base):
         time.sleep(random_base)


    for i in range(20):
        t2 = PythonOperator(
            task_id= f'task_number_is{i}',
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) / 10},
        )

        t1>>t2


