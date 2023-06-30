from datetime import datetime, timedelta

from airflow import DAG

from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    'hw_13_m-sazonov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='hw_13_m-sazonov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 27),
    catchup=False,
) as dag:
    def determine_course():
        var = Variable.get("is_startml")
        if var == "False":
            return "not_startml_desc"
        else:
            return "startml_desc"


    def startml():
        print("StartML is a starter course for ambitious people")


    def not_startml():
        print("Not a startML course, sorry")


    t1 = DummyOperator(
        task_id="before_branching"
    )

    t2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=determine_course
    )

    t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=startml
    )

    t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml
    )

    t5 = DummyOperator(
        task_id="after_branching"
    )

    t1 >> t2 >> [t3, t4] >> t5
