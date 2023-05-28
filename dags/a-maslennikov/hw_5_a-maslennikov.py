# Создайте новый DAG, состоящий из одного BashOperator
# Этот оператор должен  использовать шаблонизированную команду следующего вида:
# "Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts
# и затем распечатать значение run_id"
# Здесь ts и run_id - это шаблонные переменные
# (вспомните, как в лекции подставляли шаблонные переменные).

import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
    'hw_5_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = "Making DAG for 5th task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 26),
    catchup = False,
    tags = ["hw_5_a-maslennikov"],
) as dag:

    sample_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ts}}"
        echo "{{run_id}}"
    {% endfor %}
    """
    )
    t1 = BashOperator(
        task_id = "sample",
        bash_command = sample_command
    )

    t1