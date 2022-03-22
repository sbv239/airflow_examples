from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchingOperator


def get_condition():
    from airflow.models import Variable
    if Variable.get('is_startml') == True:
        return "startml_desc"
    return "not_startml_desc"


with DAG(
    'e_bogomolova_step_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG_for_e_bogomolova_step_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step_13'],
) as dag:

    task_1 = BashOperator(
        task_id='startml_desc',
        bash_command='echo "StartML is a starter course for ambitious people"',
    )

    task_2 = BashOperator(
        task_id='not_startml_desc',
        bash_command='echo "Not a startML course, sorry"',
    )

    task_3 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=get_condition
    )

    start = DummyOperator(
        task_id='before_branching'
    )

    finish = DummyOperator(
        task_id='after_branching'
    )

    start >> task_3 >> [task_1, task_2] >> finish
