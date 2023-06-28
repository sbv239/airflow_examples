from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

def define_branch():
        if Variable.get('is_startml') == "True":
                return 'startml_desc'
        else:
                return 'not_startml_desc'

def startml_true():
        print('StartML is a starter course for ambitious people')

def startml_false():
        print('Not a startML course, sorry')

with DAG(
    'hw_a-shulga-4_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 27),
    catchup=False
    ) as dag:

        t1 = DummyOperator(
            task_id='before_branching',
        )

        t2 = BranchPythonOperator(
        task_id='choose_course',
        python_callable=define_branch
    )
        
        t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=startml_true
    )
        t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=startml_false
    )
        t5 = DummyOperator(
            task_id='after_branching',
        )


        t1 >> t2 >> [t3, t4] >> t5