from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator



with DAG(
    'ratibor_task13',
    start_date=datetime(2023, 5, 11),
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:

    def branching_function():
        from airflow.models import Variable
        if Variable.get("is_startml") == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"
        

    branching_operator = BranchPythonOperator(
        task_id="branching_operator",
        python_callable=branching_function
    )


    def startml_desc():
        print("StartML is a starter course for ambitious people")

    
    if_startml_operator = PythonOperator(
        task_id = "startml_desc",
        python_callable=startml_desc
    )


    def not_startml_desc():
        print("Not a startML course, sorry")

    if_not_startml_operator = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml_desc
    )

    dummy_before_branching = EmptyOperator(
        task_id="dummy_before_branching"
    )
    dummy_after_branching = EmptyOperator(
        task_id="dummy_after_branching"
    )

    dummy_before_branching >> branching_operator >> dummy_after_branching


