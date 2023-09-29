"""
BranchPythonOperator
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator 



with DAG(
    'hw_13_i-daniljuk',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    description='variables',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['i-daniljuk'],
) as dag:
    
    def print_startml():
        print('StartML is a starter course for ambitious people')

    def print_not_startml():
        print('Not a startML course, sorry')
    
    def determine_course():
        from airflow.models import Variable
        is_startml = Variable.get('is_startml')
        if is_startml == 'True':
            task_id = 'startml_desc'
        else:
            task_id = 'not_startml_desc'


    positive = PythonOperator(
        task_id='startml_desc',
        python_callable=print_startml,
    )

    negative = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_startml,
    )
    
    before = DummyOperator(task_id='before_branching')
    after = DummyOperator(task_id='after_branching')
    
    determinator = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determine_course
    )
    
   
before >> determinator >> after
