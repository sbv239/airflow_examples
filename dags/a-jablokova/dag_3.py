from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'intro_3rd',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='intro_3rd',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 10),
    catchup=False,
    tags=['a-jablokova'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = 'bash_' + str(i),
            bash_command = f"echo {i}",
        )
    
    def print_num(task_number):
        print(f"task number is: {task_number}")

    for i in range(10,30):
        t2 = PythonOperator(
            task_id = 'python_' + str(i),
            python_callable = print_num,
            op_kwargs = {'task_number': i},
        )

    dag.doc_md = '''
        #THIS IS DAG
        **This is documentation** *for* `dags` with `for` *loop*
    '''
    t1 >> t2