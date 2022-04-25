from datetime import datetime, timedelta
import os
#чтоб работать с DAG импортируем класс
from airflow import DAG

# DAG состоит из операторов -кирпичиков(task)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'a.gordin_task_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
# Описание DAG (не тасок, а самого DAG)
    description='a.gordin_task_5',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 10),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['a.gordin_task_5'],
) as dag:
    for i in range(10):

        t1 = BashOperator(
            task_id='t1_' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
        )
    t1