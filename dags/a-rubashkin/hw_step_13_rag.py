from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def determine_course():
    from airflow.models import Variable
    if Variable.get("is_startml") == 'True':
        return "startml_desc"
    else:
        return "not_startml_desc"


def startml_desc():
    print('StartML is a starter course for ambitious people')


def not_startml_desc():
    print('Not a startML course, sorry')


with DAG(
    'rag_hw_13',
    description='HW_step_13',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['rag23'],
) as dag:

    t1 = DummyOperator(
        task_id='before_branching'
    )

    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determine_course,
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
    )

    t5 = DummyOperator(
        task_id='after_branching',
    )

    t1 >> t2 >> [t3, t4] >> t5
