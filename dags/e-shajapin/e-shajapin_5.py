# Для объявления DAG импортируем класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    'hw_e-shajapin_5',
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
    description='HW 5 e-shajapin',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    start_date=datetime(2023, 11, 19),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['e-shajapin_step_5'],
) as dag:

        templated_command = dedent(
                """
            {% for i in range(5) %}
                echo "{{ ts }}"
                echo "{{ run_id }}"
            {% endfor %}
            """
        )  # поддерживается шаблонизация через Jinja

        task_templated = BashOperator(
                task_id='templated',
                depends_on_past=False,
                bash_command=templated_command,
        )

        task_templated
