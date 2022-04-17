from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from psycopg2.extras import RealDictCursor
import psycopg2
from airflow.models import Variable

with DAG(
    'hw_12_a-haletina-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_12_a-haletina-7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 11),
    catchup=False,
    tags=['hw_12_a-haletina-7'],
) as dag:
    
    def get_condition():
        if Variable.get('is_startml') == 'True':
            return "startml_desc"
        return "not_startml_desc"

    def startml_desc():
            print("StartML is a starter course for ambitious people")


    def not_startml_desc():
            print("Not a startML course, sorry")

    t1 = BranchPythonOperator(
            task_id='check_course',
            python_callable=get_condition,
            trigger_rule='one_success'
    )

    t2 = PythonOperator(
            task_id='startml_desc',
            python_callable=startml_desc,
    )

    t3 = PythonOperator(
            task_id='not_startml_desc',
            python_callable=not_startml_desc,
    )

    t1 >> [t2, t3] 