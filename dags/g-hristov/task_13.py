from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


with DAG(
    'hw_g-hristov_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:
    def startml():
        return "StartML is a starter course for ambitious people"

    def notstartml():
        return "Not a startML course, sorry"

    def get_branch():
        if Variable.get("is_startml") == True:
            return "startml_desc"
        return "not_startml_desc"

    branch = BranchPythonOperator(
            task_id='determine_courses',
            python_callable=get_branch,
    )
    ml = PythonOperator(

        task_id="startml_desc",
        python_callable=startml,
    )
    noml = PythonOperator(

        task_id="not_startml_desc",
        python_callable=notstartml,
    )

    before = DummyOperator(
        task_id="before_branching"
    )

    after = DummyOperator(
        task_id="after_branching"
    )

    before>>branch>>after