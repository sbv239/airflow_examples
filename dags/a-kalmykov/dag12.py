from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def pull_xcom():
    print(Variable.get("is_startml"))


def determine_course():
    return "startml_desc" if Variable.get("is_startml") else "not_startml_desc"


def start_ml():
    print("StartML is a starter course for ambitious people")


def not_start_ml():
    print("Not a startML course, sorry")


with DAG(
        dag_id='a-kalmykov-dag-12',
        default_args=default_args,
        description='Dag 12 Kalmykov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 4),
        catchup=False,
        tags=['a-kalmykov'],
) as dag:
    start = DummyOperator(task_id='before_branching')
    branching = BranchPythonOperator(task_id='determine_course',
                                     python_callable=determine_course)
    end = DummyOperator(task_id='after_branching')
    is_ml = PythonOperator(task_id='startml_desc', python_callable=start_ml)
    not_ml = PythonOperator(task_id='not_startml_desc', python_callable=not_start_ml)

    start >> branching >> [is_ml,not_ml]>> end