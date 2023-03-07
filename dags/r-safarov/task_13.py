from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

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

           
with DAG(
    'hw11_13_r-safarov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Task_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_11_r-safarov']
) as dag:
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
