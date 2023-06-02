from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

with DAG(
        'hw_j-miller_13',
        default_args={
            'depends_on_past': False,  # Если прошлые запуски упали, надо ли ждать их успеха
            'email': ['airflow@example.com'],  # Кому писать при провале
            'email_on_failure': False,  # А писать ли вообще при провале?
            'email_on_retry': False,  # Писать ли при автоматическом перезапуске по провалу
            'retries': 1,  # Сколько раз пытаться запустить, далее помечать как failed
            'retry_delay': timedelta(minutes=5),  # Сколько ждать между перезапусками# timedelta из пакета datetime
        },
        description='A 3 simple tutorial DAG',  # Описание DAG (не тасок, а самого DAG)
        schedule_interval=timedelta(days=1),  # Как часто запускать DAG
        start_date=datetime(2023, 6, 1),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        tags=['SimJul_13'],  # теги, способ помечать даги
) as dag:
    #Создайте DAG, имеющий BranchPythonOperator. Логика ветвления должна быть следующая:
    # если значение Variable is_startml равно "True", то перейти в таску с task_id="startml_desc",
    # иначе перейти в таску с task_id="not_startml_desc".
    # Затем объявите две задачи с task_id="startml_desc" и task_id="not_startml_desc".
    def print_is_startml():
        is_startml = Variable.get("is_startml")
        if is_startml == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    task1 = BranchPythonOperator(
    task_id='print_is_startml',
    python_callable=print_is_startml,
    dag=dag
    )

    task2 = PythonOperator(
    task_id='startml_desc',
    python_callable=lambda: print("StartML is a starter course for ambitious people"),
    dag=dag
    )

    task3 = PythonOperator(
    task_id='not_startml_desc',
    python_callable=lambda: print("Not a StartML course, sorry"),
    dag=dag
    )

    dummy1 = DummyOperator(
        task_id='start',
        dag=dag
    )

    dummy2 = DummyOperator(
        task_id='end',
        dag=dag
    )

    dummy1>>task1>>[task2,task3]>>dummy2
