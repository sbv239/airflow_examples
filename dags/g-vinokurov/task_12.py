from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


def determine_course():
    switcher = Variable.get("is_startml")

    if switcher == 'True':
        return "startml_desc"
    return "not_startml_desc"

def startml_desc():
    print("StartML is a starter course for ambitious people")

def not_startml_desc():
    print("Not a startML course, sorry")

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('hw_g-vinokurov_12',
    default_args=default_args,
        description='DAG in task_12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 29),
        catchup=False,
) as dag:

    operator1 = DummyOperator(
        task_id='before_branching'
    )

    operator2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=determine_course,
    )

    operator3 = PythonOperator(
        task_id="startml_desc",
        python_callable=startml_desc,
    )

    operator4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml_desc,
    )

    operator5 = DummyOperator(
        task_id="after_branching"
    )

    operator1 >> operator2 >> [operator3, operator4] >> operator5