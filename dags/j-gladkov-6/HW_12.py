from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta
from textwrap import dedent

def startml():
    print("StartML is a starter course for ambitious people")

def notstartml():
    print("Not a startML course, sorry")

def chose_your_destiny():
    from airflow.models import Variable
    if Variable.get("is_startml") return 'startml_desc' else return 'not_startml_desc'

with DAG(
    'hw_12_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "Variables & Branching", # name
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 22),
    catchup = False,
    tags = ['hw_12'],
) as dag:

    t_begin = DummyOperator(
        task_id='where_do_i_begin',
    )

    t_choice = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = chose_your_destiny,
        )

    t_first = PythonOperator(
        task_id="startml_desc",
        python_callable = startml,
        )

    t_second = PythonOperator(
        task_id="not_startml_desc",
        python_callable = notstartml,
        )

    t_final = DummyOperator(
        task_id='final',
    )

    t_begin >> t_choice >> [t_first, t_second] >> t_final
