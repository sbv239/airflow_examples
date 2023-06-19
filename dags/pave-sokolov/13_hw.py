from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


def choose_branch ():
    if Variable.get('is_startml') == "True":
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def sml():
    print ("StartML is a starter course for ambitious people")
    
def nsml():
    print ("Not a startML course, sorry")
    

with DAG ('hw_pave-sokolov_13',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
            },
            description= 'HW 13 DAG',
            schedule_interval=timedelta(days = 1),
            start_date= datetime(2023,6,12),
            catchup= False,
            tags= ['example']
        ) as dag:
    
    before_task = DummyOperator(
        task_id = "before_branching"        
    )

    after_task = DummyOperator(
        task_id = "after_branching"        
    )

    branching_task = BranchPythonOperator(
        task_id = "determine_course",
        python_callable = choose_branch
    )

    start_ml_task = PythonOperator(
        task_id = "startml_desc",
        python_callable = sml
    )

    not_start_ml_task = PythonOperator(
        task_id = "not_startml_desc",
        python_callable = nsml
    )

    before_task >> branching_task >> start_ml_task >> after_task
    before_task >> branching_task >> not_start_ml_task >> after_task