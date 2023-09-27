"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    description="A simple tutorial DAG",
    dag_id="ale-kim_dag_12",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["ale-kim"],
) as dag:

    def print_variable():
        print(Variable.get("is_startml"))
        return "--------- variable print -------"

    first_oper = PythonOperator(
        task_id="print_variable",
        python_callable=print_variable,
    )

    first_oper.doc_md = dedent(
        """\
    # Task Documentation
    print variable "is_startml"

    """
    )
