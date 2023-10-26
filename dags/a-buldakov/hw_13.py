from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta


def branch_choice():
    if Variable.get("is_startml") is "True":
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def left_task():
    print("StartML is a starter course for ambitious people")


def right_task():
    print("Not a startML course, sorry")

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'hw_13_a-buldakov',
    default_args = default_args,
    start_date = datetime.now(),
    tags=['a-buldakov']
)

hw_login_0 = DummyOperator(
    task_id='start_branching',
    dag = dag
)

branching = BranchPythonOperator(
    task_id='determine_course',
    python_callable=branch_choice,
    dag = dag
)

hw_login_1 = PythonOperator(
    task_id="startml_desc",
    python_callable=left_task,
    dag = dag
)

hw_login_2 = PythonOperator(
    task_id="not_startml_desc",
    python_callable=right_task,
    dag = dag
)

hw_login_3 = DummyOperator(
    task_id='end_branching',
    dag = dag
)

hw_login_0 >> branching >> [hw_login_1, hw_login_2] >> hw_login_3