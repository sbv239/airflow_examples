from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return "startml_desc"
    return "not_startml_desc"


def print_startml():
    print("StartML is a starter course for ambitious people")


def print_not_startml():
    print("Not a startML course, sorry")


with DAG(
        'hw_13_s-plaksin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Variables_and_branching',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 28),
        catchup=False,
        tags=['hw_13'],
) as dag:
    start = DummyOperator(
        task_id="start_branching",
    )
    determine_course = BranchPythonOperator(
        task_id='course',
        python_callable=get_variable
    )
    not_startml_desc = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_startml
    )
    startml_desc = PythonOperator(
        task_id='startml_desc',
        python_callable=print_startml
    )
    end = DummyOperator(
        task_id="end_branching",
    )

start >> determine_course >> [not_startml_desc, startml_desc] >> end
