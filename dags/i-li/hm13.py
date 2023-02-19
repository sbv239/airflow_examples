from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable


def get_sql():
    is_prod = Variable.get('is_startml')
    if is_prod:
        return 'startml_desc'
    return 'not_startml_desc'

def print_true():
    print("StartML is a starter course for ambitious people")

def print_false():
    print("Not a startML course, sorry")

with DAG(
        'hm_13_i-li',
        default_args={
            'dependes_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_in_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2023, 2, 15)
) as dag:
    t1 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=get_sql
    )
    t2 = PythonOperator(
        task_id='startml_desc',
        python_callable=print_true
    )

    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_false
    )
    t4 = EmptyOperator(
        task_id='before_branching'
    )

    t4 >> t1
