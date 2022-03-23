from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_variable():
    from airflow.models import Variable
    print(Variable.get('is_startml'))


with DAG(
    'e_bogomolova_step_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG_for_e_bogomolova_step_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step_12'],
) as dag:

    t1 = PythonOperator(
        task_id='variable_is_startml',
        python_callable=get_variable,
    )
    t1
