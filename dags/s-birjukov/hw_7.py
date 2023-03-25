
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(

    'hw_7_s-birjukov',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='hw_11_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 25),
    catchup=False,
) as dag:


    def print_context (task_number,ts,run_id):
        #добавили аргументы в функцию и распечатали их
        print(ts)
        print(run_id)
        #вернули task_number
        return  task_number

    for task_number in range(20):
        t2 = PythonOperator(
            task_id=f'python_cycle_number_{task_number}',
            python_callable=print_context,
            #передали в kwargs параметр "task_number"
            op_kwargs={'task_number': task_number}
        )



