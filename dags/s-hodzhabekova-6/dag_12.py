from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


def choose_branch():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml:
        return ["startml_desc"]
    return ['not_startml_desc']


def startml():
    print("StartML is a starter course for ambitious people")


def not_startml():
    print("Not a startML course, sorry")


with DAG(
    'hw_12_s-hodzhabekova-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_12', 'khodjabekova'],
) as dag:

    run_this_first = DummyOperator(
        task_id='run_this_first',
    )

    t1 = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch,
    )
    t2 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml,
    )

    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml,
    )

    run_this_last = DummyOperator(
        task_id='run_this_last',
    )
    run_this_first >> t1 >> [t2, t3] >> run_this_last
