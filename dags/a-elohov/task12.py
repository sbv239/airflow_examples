from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


def printer(**kwargs):
    print(kwargs['my_message'])
    return


def decision_function(**kwargs):
    is_startml = Variable.get("is_startml")
    return "startml_desc" if is_startml == "True" else "not_startml_desc"


with DAG(
    'a_elokhov_task_12',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
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
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = DummyOperator(
        task_id='before_branching'
    )

    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=decision_function
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=printer,
        op_kwargs={'my_message': "StartML is a starter course for ambitious people"}
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=printer,
        op_kwargs={'my_message': "Not a startML course, sorry"}
    )

    t5 = DummyOperator(
        task_id='after_branching'
    )

    t1 >> t2 >> [t3, t4] >> t5
