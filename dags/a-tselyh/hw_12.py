from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator


def get_condition():
    from airflow.models import Variable
    if Variable.get('is_startml') == 'True':
        return "startml_desc"
    else:
        return "not_startml_desc"


with DAG(
    'a-tselyh_dag_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='final_dag_step_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['oh_my_dag_13'],
) as dag:

    task_0 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=get_condition,

    )

    task_1 = BashOperator(
        task_id='startml_desc',
        bash_command='echo "StartML is a starter course for ambitious people"',
    )

    task_2 = BashOperator(
        task_id='not_startml_desc',
        bash_command='echo "Not a startML course, sorry"',
    )

    begin = DummyOperator(
        task_id='start_branching'
    )

    end = DummyOperator(
        task_id='finish_branching'
    )

    begin >> task_0 >> [task_1, task_2] >> end