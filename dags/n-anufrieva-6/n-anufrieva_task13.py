from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable


def determine_course():
    if Variable.get("is_startml"):
        return 'startml_desc'
    return 'not_startml_desc'


with DAG(
        'n-anufrieva_task13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='n-anufrieva_task13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['n-anufrieva_task13'],
) as dag:

    before_branching = DummyOperator(
        task_id='before_branching',
    )

    get_course = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determine_course,
    )

    task_1 = BashOperator(
        task_id='startml_desc',
        bash_command="echo StartML is a starter course for ambitious people",
    )

    task_2 = BashOperator(
        task_id='not_startml_desc',
        bash_command="echo Not a startML course, sorry",
    )

    after_branching = DummyOperator(
        task_id='after_branching',
    )

    before_branching >> get_course >> [task_1, task_2] >> after_branching
