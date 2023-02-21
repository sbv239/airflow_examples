"""
#### Task Documentation

"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'a-azarova-17_hw5',
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
    description="""Шаблонизация
    Создайте новый DAG, состоящий из одного BashOperator. 
    Этот оператор должен  использовать шаблонизированную команду следующего вида: 
    "Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts 
    и затем распечатать значение run_id". Здесь ts и run_id - это шаблонные переменные 
    (вспомните, как в лекции подставляли шаблонные переменные).
    """,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['azarova'],
) as dag:



    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )  # поддерживается шаблонизация через Jinja
    # https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#concepts-jinja-templating

    t1 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    # последовательность задач
    t_1
