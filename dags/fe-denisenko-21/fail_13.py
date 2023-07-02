from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
def branch():
    is_prod = Variable.get("is_startml")
    if is_prod == 'True':
        return "startml_desc"
    else:
        return "not_startml_desc"
def function_t1():
    return print('StartML is a starter course for ambitious people')
def function_t2():
    return print('Not a startML course, sorry')
# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_fe-denisenko-21_13_step',
    start_date=datetime(2023, 6, 20),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    make_request = BranchPythonOperator(
        task_id='make_request',
        python_callable=branch,
    )
    t2 = PythonOperator(
        task_id='startml_desc',
        python_callable=function_t1
    )
    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=function_t2
    )
    make_request >> [t2, t3]
