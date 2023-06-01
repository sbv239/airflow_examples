from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_j-miller_5',
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
        start_date=datetime(2023, 5, 31),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        tags=['SimJul_5'],  # теги, способ помечать даги
) as dag:

    t1 = BashOperator(
        task_id='shablone_echo',  # id, будет отображаться в интерфейсе
        bash_command= dedent(
            """
        {% for i in range(0,4) %}
            echo "{{ ts }}"
            echo "{{run_id}}"
        {% endfor %}
        """
        ))
