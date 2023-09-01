from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

from datetime import timedelta, datetime


def check_variable():
    is_startml = Variable.get("is_startml")
    if is_startml == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"


def startml_desc():
    print("StartML is a starter course for ambitious people")


def not_startml_desc():
    print("Not a startML course, sorry")


with DAG(
    'hw_e-zastrogina_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
            },
    start_date=datetime(2023, 6, 26),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    t00 = DummyOperator(
        task_id='before_branching'
    )

    t0 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=check_variable
    )

    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc
    )

    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc
    )

    t33 = DummyOperator(
        task_id='after_branching'
    )

    t00 >> t0 >> [t1, t2] >> t33
