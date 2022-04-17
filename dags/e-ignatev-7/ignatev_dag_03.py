from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'ignatev_dag_03',
    default_args=default_args,
    start_date=datetime(2022, 4, 15),
    max_active_runs=1,
    schedule_interval=timedelta(days = 1),
) as dag:

    dag.doc_md = '''
    ### ignatev_dag_02 documentation

    This dag fulfils the requirements for *task 03*, *lesson 13* in **StartML** course
    '''

    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )

    t1.doc_md = '''
    ### t1 task documentation

    This task fulfils the requirements for *task 03*, *lesson 13* in **StartML** course
    This group of tasks is created with `for` loop and `BashOperator`
    '''

    def print_task(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_' + str(i),
            python_callable=print_task,
            op_kwargs={'task_number':i}
        )

    t2.doc_md = '''
    ### t2 task documentation

    This task fulfils the requirements for *task 03*, *lesson 13* in **StartML** course
    This group of tasks is created with `for` loop and `PythonOperator`
    '''