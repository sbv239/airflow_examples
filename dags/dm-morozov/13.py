from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator


def choose_branch(datum):
    if datum == "True":
        return "startml_desc"
    return "not_startml_desc"


def print_if_true():
    print("StartML is a starter course for ambitious people")


def print_if_false():
    print("Not a startML course, sorry")


with DAG(
        '13_dm-morozov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 2, 14)
) as dag:
    t1 = DummyOperator(
        task_id='before_branching'
    )

    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=choose_branch,
        op_kwargs={'datum': Variable.get('is_startml')}
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=print_if_true
    )

    t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_if_false
    )

    t5 = DummyOperator(
        task_id='after_branching'
    )

    t1 >> t2 >> [t3, t4] >> t5