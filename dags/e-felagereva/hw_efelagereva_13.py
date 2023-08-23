from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

def branch():
    is_startml = Variable.get("is_startml")
    if is_startml :
        return "startml_desc"
    else:
        return "not_startml_desc"

def print_task1():
    print("StartML is a starter course for ambitious people")

def print_task2():
    print("Not a startML course, sorry")

with DAG(
    'hw_efelagereva_13',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'a simple dag with BranchPythonOperator',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 22)
) as dag:
    start_task = DummyOperator(
        task_id = 'start_task'
    )
    t1 = BranchPythonOperator(
        task_id = 'branch_task',
        python_callable = branch,
        provide_context = True
    )
    t2 = BranchPythonOperator(
        task_id = 'startml_desc',
        python_callable = print_task1,
        provide_context = True
    )
    t3 = BranchPythonOperator(
        task_id = 'not_startml_desc',
        python_callable = print_task2,
        provide_context = True
    )
    end_task  = DummyOperator(
        task_id = 'end_task'
    )

    t2.set_downstream(t1)
    t3.set_downstream(t1)
