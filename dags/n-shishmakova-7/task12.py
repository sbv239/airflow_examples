from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


def get_decision():
    if Variable.get("is_startml") == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"


def get_info_startml():
    print("StartML is a starter course for ambitious people")


def get_info_not_startml():
    print("Not a startML course, sorry")


with DAG(
        'hw_12_n-shishmakova-7',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='DAG for 12 task in lesson 11',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 1),
        # Запустить за старые даты относительно сегодня
        catchup=False,
        # теги, способ помечать даги
        tags=['hw12'],
) as dag:
    t1 = DummyOperator(
        task_id='dummy_start'
    )
    t2 = BranchPythonOperator(
        task_id='get_decision',  # нужен task_id, как и всем операторам
        python_callable=get_decision
    )
    t3 = PythonOperator(
        task_id='startml_desc',  # нужен task_id, как и всем операторам
        python_callable=get_info_startml
    )
    t4 = PythonOperator(
        task_id='not_startml_desc',  # нужен task_id, как и всем операторам
        python_callable=get_info_not_startml
    )
    t5 = DummyOperator(
        task_id='dummy_finish'
    )

    t1 >> t2 >> [t3, t4] >> t5
