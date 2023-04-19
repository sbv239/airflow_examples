from datetime import datetime, timedelta
from textwrap import dedent

from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator



def choose_path():
    from airflow.models import Variable

    if Variable.get("is_startml") == "True":
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def startml():
    print("StartML is a starter course for ambitious people")
    return "Something"

def not_startml():
    print("Not a startML course, sorry")
    return "Something"

with DAG(
    "BranchingOperatorTest",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description= "first try of starting DAG's",
    schedule_interval= timedelta(days=1),
    start_date= datetime(2023, 4, 17),
    catchup= False,
    tags=['idk13']
) as dag:

    t1 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=choose_path,
        trigger_rule="one_success"
    )
    TrueStartML = PythonOperator(
        task_id='startml_desc',
        python_callable=startml
    )

    FalseStartML = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml
    )

    t1 >> [TrueStartML, FalseStartML]
