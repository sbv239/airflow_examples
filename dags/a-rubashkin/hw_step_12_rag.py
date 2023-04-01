from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_variable():
    res = Variable.get('is_startml')
    print(res)


with DAG(
    'rag_hw_12',
    description='HW_step_12',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['rag23'],
) as dag:

    t_1 = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable,
    )

    t_1
