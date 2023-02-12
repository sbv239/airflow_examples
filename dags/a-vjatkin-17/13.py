from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable


def branch_selector():
    variable = Variable.get("is_startml")

    if variable == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"


def first_task():
    print("StartML is a starter course for ambitious people")

def second_task():
    print("Not a startML course, sorry")


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    'a-vjatkin-17_task_13',
    default_args=default_args,
    description='test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 12),
    catchup=False
) as dag:

    t1 = BranchPythonOperator(
        task_id="branch_example",
        python_callable=branch_selector
    )

    t2 = PythonOperator(
        task_id="startml_desc",
        python_callable=first_task
    )

    t3 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=second_task
    )

    t1 >> [t2, t3]
