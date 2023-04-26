"""
step_13 DAG
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator, BranchPythonOperator


with DAG(
    'hw_r-shahvaly_13',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for step_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['r-shahvaly'],
) as dag:

        def get_choice():
                return "startml_desc" if Variable.get("is_startml") == 'True' else "not_startml_desc"

        def print_ml():
                print("StartML is a starter course for ambitious people")

        def print_notml():
                print("Not a startML course, sorry")


        branch = BranchPythonOperator(
                task_id="determine_course",
                python_callable=get_choice,
        )

        task_ml = PythonOperator(
                task_id="startml_desc",
                python_callable=print_ml,
        )

        task_notml = PythonOperator(
                task_id="not_startml_desc",
                python_callable=print_notml,
        )

        branch >> [task_ml, task_notml]