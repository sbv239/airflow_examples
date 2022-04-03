from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import Variable

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


def choose_id():
    is_startml = Variable.get("is_startml")
    if is_startml:
        return "startml_desc"
    return "not_startml_desc"


def print_isml():
    print("StartML is a starter course for ambitious people")


def print_notml():
    print("Not a startML course, sorry")


with DAG(
    'hw_13_d.alenin-10',
    default_args=default_args,
    description='Simple first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    t1 = DummyOperator(
        task_id="before_branching"
    )

    t2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=choose_id,
    )

    t3 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_notml,
    )

    t4 = PythonOperator(
        task_id="startml_desc",
        python_callable=print_isml,
    )

    t5 = DummyOperator(
        task_id="after_branching",
    )

    t1 >> t2 >> [t3, t4] >> t5
