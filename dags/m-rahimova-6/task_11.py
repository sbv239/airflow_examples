"""

Напишите DAG, состоящий из одного PythonOperator.
Этот оператор должен печатать значение Variable с названием is_startml

"""

from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator


def get_var():
    from airflow.models import Variable

    is_startml = Variable.get('is_startml')
    print(is_startml)


with DAG(
        'rakhimova_task11',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='DAG11 Rakhimova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['hehe'],
) as dag:

    t1 = PythonOperator(
        task_id='get_is_startml',
        python_callable=get_var,
    )
