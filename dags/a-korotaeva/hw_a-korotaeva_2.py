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
    catchup=False,
    tags=['example']
) as dag:

    for i in range(10):

        t1 = BashOperator(task_id='print_current_directory'+str(i), bash_command=f'echo {i}')

    def print_(**kwargs):
        print('task number is: {kw}'.format(kw=kwargs['my_keyword']))


    for j in range(20):

        t2 = PythonOperator(task_id='print_'+str(j), python_callable=print_, op_kwargs={'my_keyword': j})

    t1 >> t2