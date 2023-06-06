from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'hw_task_5_o-kochetygova',
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
        description='hw_task_4_o-kochetygova_DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 31),
        catchup=False,
        tags=['task_5'],
) as dag:
    temleted_command = dedent(
       """
    {% for i in range(0,5) %}:
            echo "{{ts}}"
            echo "{{run_id}}"
    {% endfor %}
    """
    )
    task = BashOperator(
        task_id='temleted',# нужен task_id, как и всем операторам
        bash_command=temleted_command,
    )
