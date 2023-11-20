# Для объявления DAG импортируем класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


with DAG(
    'hw_e-shajapin_6',
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
    description='DAG`s Shayapin for StartML_Step6',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    start_date=datetime(2023, 11, 20),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['e-shajapin_step_6'],
) as dag:

    # Генерируем таски в цикле
    for i in range(10):
        task_bash = BashOperator(
            task_id=f"print_echo_{i}",  # id, будет отображаться в интерфейсе
            bash_command="echo $NUMBER",  # какую bash команду выполнить в этом таске
            env={"NUMBER": i},  # задает переменные окружения
        )

        task_bash
