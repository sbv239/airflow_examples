from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

def variables():
    print(Variable.get("is_startml"))

with DAG(
    'hw_12_e-dm_dag',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description='lesson_11_task_12',
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['hw_12_e-dm_tag'],
) as dag:

    t1 = PythonOperator(
        task_id = 'variables',
        python_callable = variables
    )
	# Последовательность задач:
    t1