from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.branch import BranchPythonOperator
# from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def choose_branch():
    is_startml = Variable.get("is_startml")
    return "startml_desc" if is_startml == "True" else "not_startml_desc"


with DAG(
    'hw_alekse-smirnov_13',
    default_args=default_args,
    description='DAG for Lesson #11 Task #13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 16),
    catchup=False,
    tags=['alekse-smirnov'],
) as dag:
    start_task = DummyOperator(
        task_id="start_branching_task",
    )

    branching_task = BranchPythonOperator(
        task_id="branching_task",
        python_callable=choose_branch
    )

    is_startml_task = PythonOperator(
        task_id="startml_desc",
        python_callable=(lambda _: print("StartML is a starter course for ambitious people"))
    )

    not_startml_task = PythonOperator(
        task_id="not_startml_desc",
        python_callable=(lambda _: print("Not a startML course, sorry"))
    )

    end_task = DummyOperator(
        task_id="end_branching_task",
    )

    start_task >> branching_task >> [is_startml_task, not_startml_task] >> end_task
