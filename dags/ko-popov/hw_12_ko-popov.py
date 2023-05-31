"""
Напишите DAG, состоящий из одного PythonOperator.
Этот оператор должен печатать значение Variable с названием is_startml.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from psycopg2.extras import RealDictCursor
from airflow.models import Variable

#is_prod = Variable.get("is_prod")  # необходимо передать имя, заданное при создании Variable
# теперь в is_prod лежит значение Variable
def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)

with DAG(
    'hw_12_ko-popov',
    default_args={
        'depends_on_past': False,
        'email': {'mdkonstantinp@gmail.com'},
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hw_12_ko-popov dag',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 5, 29),
    catchup=False,
    tags = ['hw_12_ko-popov'],
) as dag:
    t1 = PythonOperator(
        task_id = "ko-popov_variable",
        python_callable = get_variable,
    )