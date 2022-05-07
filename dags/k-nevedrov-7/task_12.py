from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

with DAG(
    'k-nevedrov-7-task_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:
    before_branching_t = DummyOperator(
        task_id='before_branching',  
    )

    def determine_course():
        is_startml = Variable.get("is_startml")
        
        if is_startml == "True":
            return 'startml_desc'
        
        return 'not_startml_desc'

    determine_course_t = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determine_course
    )

    def startml_desc():
        print("StartML is a starter course for ambitious people")

    startml_desc_t = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc
    )

    def not_startml_desc():
        print("Not a startML course, sorry")

    not_startml_desc_t = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc
    )    

    after_branching_t = DummyOperator(
        task_id='after_branching' 
    )

    before_branching_t >> determine_course_t >> [startml_desc_t, not_startml_desc_t] >> after_branching_t