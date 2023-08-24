from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def choose_path():
    from airflow.models import Variable
    if Variable.get("is_startml") == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"


def print_if_true():
    return "StartML is a starter course for ambitious people"


def print_if_false():
    return "Not a startML course, sorry"


with DAG(
        'hw_n-shishkin_13',
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
        catchup=False,
        tags=['hw_n-shishkin_13'],
) as dag:
    decision = BranchPythonOperator(
        task_id="decide_path",
        python_callable=choose_path
    )
    dummy_start = DummyOperator(
        task_id="start_graph"
    )
    dummy_end = DummyOperator(
        task_id="end_graph"
    )
    if_true = PythonOperator(
        task_id="startml_desc",
        python_callable=print_if_true
    )
    if_false = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_if_false
    )
    dummy_start >> decision >> [if_true, if_false] >> dummy_end
