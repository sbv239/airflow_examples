from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG('AUshakova_HW_5',
# Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
            # Кому писать при провале
        'email': ['airflow@example.com'],
            # А писать ли вообще при провале?
        'email_on_failure': False,
            # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
            # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
            # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
            # Описание DAG (не тасок, а самого DAG)
    description='5 homework',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 6),
    catchup=False,
    tags=['hw_5'], ) as dag:

    for i in range(10):
        task_id = BashOperator(task_id = 'env_' + str(i),  # id, будет отображаться в интерфейсе
                               bash_command = "echo $NUMBER",  # какую bash команду выполнить в этом таске
                               env = {"NUMBER" : i}, )
        task_id
    
