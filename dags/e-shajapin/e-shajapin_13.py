from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable


with DAG(
    'hw_13_e-shajapin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
) as dag:

    t1 = DummyOperator(task_id='before_branching')

    def branching_rule():
        choice = Variable.get("is_startml")
        return "startml_desc" if choice == 'True' else "not_startml_desc"


    t2 = BranchPythonOperator(task_id="determine_course", python_callable=branching_rule)


    def print_start():
        print("StartML is a starter course for ambitious people")


    t3 = PythonOperator(task_id="startml_desc", python_callable=print_start)


    def print_not_start():
        print("Not a startML course, sorry")


    t4 = PythonOperator(task_id="not_startml_desc", python_callable=print_not_start)

    t5 = DummyOperator(task_id='after_branching')

    t1 >> t2 >> [t3, t4] >> t5
