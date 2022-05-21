"""
DAG: conditions and BranchingOperator
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


def choose_branch():
    from airflow.models import Variable
    if Variable.get("is_startml") == "True":
        return 'startml_desc'
    return 'not_startml_desc'


with DAG(
    'hw_12_e-leonenkov',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG with BranchPythonOperator and DummyOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['ex11'],
) as dag:

    begin = DummyOperator(
        task_id='before_branching'
    )


    branching = BranchPythonOperator(
        task_id='determine_course',
        python_callable=choose_branch
    )


    t1 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=lambda: print("Not a startML course, sorry")
    )


    t2 = PythonOperator(
        task_id='startml_desc',
        python_callable=lambda: print("StartML is a starter course for ambitious people")
    )


    end = DummyOperator(
        task_id='after_branching'
    )


    begin >> branching >> [t1, t2] >> end