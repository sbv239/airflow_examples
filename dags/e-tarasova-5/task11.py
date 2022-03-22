from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator


def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
        'tarasova_task11',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='DAG for task 11 Tarasova E',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 19),
        catchup=False,
        tags=['task11']

        ,
) as dag:
    t1 = PythonOperator(
        task_id='is_startml',
        bash_command=get_variable,
    )

    t1
