from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


def branching():
    def print_variables():
        is_startml = Variable.get("is_startml")
        return is_startml
    if print_variables() == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"


def print_true_var():
    print("StartML is a starter course for ambitious people")


def print_false_var():
    print("Not a startML course, sorry")


with DAG(
        'aakulzhanov_task_13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple Task 13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    start_dummy = DummyOperator(
        task_id="before_branching"
    )
    task_branch = BranchPythonOperator(
        task_id="determine_course",
        python_callable=branching
    )
    task_true = PythonOperator(
      task_id="startml_desc",
      python_callable=print_true_var
    )
    task_false = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_false_var
    )
    end_dummy = DummyOperator(
        task_id="after_branching",
        trigger_rule="one_success"
    )
start_dummy >> task_branch >> [task_true, task_false] >> end_dummy
