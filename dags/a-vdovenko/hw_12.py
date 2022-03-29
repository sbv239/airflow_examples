from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


def choose_branch():
    from airflow.models import Variable
    if Variable.get('is_startml'):
        return 'startml_desc'
    else:
        return 'not_startml_desc'


with DAG(
    'hw_12_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="Lesson 11 home work 12",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    t1 = DummyOperator(
        task_id='before_branching'
    )
    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=choose_branch
    )
    t3 = BashOperator(
        task_id='startml_desc',
        bash_command='echo "StartML is a starter course for ambitious people"'
    )
    t4 = BashOperator(
        task_id='not_startml_desc',
        bash_command='echo "Not a startML course, sorry"'
    )
    t5 = DummyOperator(
        task_id='after_branching'
    )
    t1 >> t2 >> [t3, t4] >> t5
