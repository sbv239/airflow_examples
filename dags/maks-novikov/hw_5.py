from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from textwrap import dedent
def check_num(task_number):
    return f'task number is: {task_number}'

    
with DAG(
    'hw_maks-novikov_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='HW5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 16),
    catchup=False,
    tags=['hw_maks-novikov_5'],
) as dag:
    dag.doc_md = """
        This is a documentation placed anywheree
    """ 

    for i in range(10):
        #number = i
        t1 = BashOperator(
            task_id='task_' + str(i),
            env={'NUMBER': str(i)},
            bash_command='echo $NUMBER',
            dag=dag
        )    
  

    t1