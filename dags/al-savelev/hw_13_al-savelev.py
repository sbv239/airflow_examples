from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta
from textwrap import dedent

from airflow.models import Variable

with DAG(
    'hw_13_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['hw_al-savelev']
) as dag:

    def determine_course():
        startml = Variable.get("is_startml")
        if startml == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"
    
    def print_1():
        print("StartML is a starter course for ambitious people")
    
    def print_2():
        print("Not a startML course, sorry")


    t1 = DummyOperator(
        task_id="before_branching"
    )
    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determine_course
    )
    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=print_1
    )
    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_2
    )
    t5 = DummyOperator(
        task_id='after_branching'
    )
    
    t1 >> t2 >> [t3, t4] >> t5
