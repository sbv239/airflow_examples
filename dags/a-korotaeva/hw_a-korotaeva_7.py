from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_2_a-korotaeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG', schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 13),
    catchup=False
) as dag:
    def print_(ts, run_id, **kwargs):
        task_number = kwargs['task_number']
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)
        return 'Whatever you return gets printed in the logs'


    for j in range(11, 31):

        t2 = PythonOperator(task_id='print_'+str(j), python_callable=print_, op_kwargs={'task_number': j})

    t2