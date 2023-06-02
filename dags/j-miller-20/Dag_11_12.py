from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

#Напишите DAG, состоящий из одного PythonOperator.
# Этот оператор должен печатать значение Variable с названием is_startml.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_j-miller_12',
        description='A 10 simple tutorial DAG wtih Xcom',  # Описание DAG (не тасок, а самого DAG)
        schedule_interval=timedelta(days=1),  # Как часто запускать DAG
        start_date=datetime(2023, 6, 1),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        tags=['SimJul_12'],  # теги, способ помечать даги
) as dag:

    def print_is_startml():
        is_startml = Variable.get("is_startml")
        print(f"Value of is_startml:", is_startml)

    task= PythonOperator(
        task_id='print_is_startml',
        python_callable=print_is_startml,
        # op_kwargs={'xcom_data': 'xcom test'},
        provide_context=True
    )
