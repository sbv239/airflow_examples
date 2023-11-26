from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
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

with DAG(
    'hw_kamilahmadov_12',
    default_args=default_args,
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_12"]
) as dag:

    def branch_call(**kwargs):
        is_startml = Variable.get("is_startml")
        if is_startml.lower() == "true":
            return "startml_desc"
        else:
            return "not_startml_desc"

    def ml_true_call(**kwargs):
        print("StartML is a starter course for ambitious people")

    def ml_false_call(**kwargs):
        print("Not a StartML course, sorry")

    determine_course = BranchPythonOperator(
        task_id="determine_course",
        python_callable=branch_call
    )

    startml_desc = PythonOperator(
        task_id="startml_desc",
        python_callable=ml_true_call,
    )

    not_startml_desc = PythonOperator(
        task_id="not_startml_desc",
        python_callable=ml_false_call,
    )

    dummy_start = DummyOperator(
        task_id="before_branching"
    )

    dummy_end = DummyOperator(
        task_id="after_branching"
    )

    dummy_start >> determine_course
    determine_course >> [startml_desc, not_startml_desc]
    [startml_desc, not_startml_desc] >> dummy_end
