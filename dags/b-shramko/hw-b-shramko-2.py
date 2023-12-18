from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('hw-b-shramko-2',
         default_args={
             'depends_on_past': False,
             'email': ['airflow@example.com'],
             'email_on_failure': False,
             'email_on_retry': False,
             'retries': 1,
             'retry_delay': timedelta(minutes=5),
         },
         description='DAG_2',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022, 1, 1),
         catchup=False,
         tags=['example'],
         ) as dag:
    t1 = BashOperator(
        task_id='print dir',
        bash_command='pwd',
    )


    def print_context(ds, **kwargs):
        print(ds)
        print("One more print by SBV")


    t2 = PythonOperator(
        task_id='print ds',
        python_callable=print_context
    )
