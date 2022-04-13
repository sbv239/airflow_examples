from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

def get_condition():
    if Variable.get('is_startml'):
        return "startml_desc"
    return "not_startml_desc"

with DAG(
    'g-sheverdin-7_task12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='g-sheverdin-7_DAG_task12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 11),
    catchup=False,
    tags=['g-sheverdin-7-task12'],
) as dag:

    def startml_desc():
            print("StartML is a starter course for ambitious people")


    def not_startml_desc():
            print("Not a startML course, sorry")

    start = DummyOperator(
            task_id='before_branching'
    )

    t1 = BranchPythonOperator(
            task_id='check_course',
            python_callable=get_condition,
            trigger_rule='one_success'
    )

    t2 = PythonOperator(
            task_id='startml_desc',
            python_callable=startml_desc,
    )

    t3 = PythonOperator(
            task_id='not_startml_desc',
            python_callable=not_startml_desc,
    )

    finish = DummyOperator(
            task_id='after_branching'
    )

    start >> t1 >> [t2, t3] >> finish
