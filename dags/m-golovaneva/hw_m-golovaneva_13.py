from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator


def get_variable():
    from airflow.models import Variable
    is_startml= Variable.get("is_startml")
    print(is_startml)
    return is_startml

def branch_clause():
    from airflow.models import Variable
    if Variable.get("is_startml") == True:
        return "startml_desc"
    else:
        return "not_startml_desc"


with DAG(
        "hw_m-golovaneva_task11",

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Connections in Airflow (to Postgres)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,

        tags=['mariaSG']
) as dag:

    getting_var = PythonOperator(
        task_id="getting_var_example",
        python_callable=get_variable
    )

    brancher = BranchPythonOperator(
        task_id="brancher",
        python_callable=branch_clause
    )

    def startml_desc():
        print("StartML is a starter course for ambitious people")
    startml_desc = PythonOperator(
        task_id="startml_desc",
        python_callable=startml_desc
    )

    def not_startml_desc():
        print("not_startml_desc")
    not_startml_desc = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml_desc
    )







