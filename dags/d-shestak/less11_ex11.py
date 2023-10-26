from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow.models import Variable

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_11',
         default_args=default_args,
         description='hw_d-shestak_11',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_11_d-shestak']
         ) as dag:

    def print_variable():
        print(Variable.get('is_startml'))
        return 'print from Variable'


    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable
    )