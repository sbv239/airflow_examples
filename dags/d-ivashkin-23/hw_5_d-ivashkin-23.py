from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_d-ivashkin-23_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
        description='Homework 5-th step DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 21),
        catchup=False,
        tags=['homework', 'di']
) as dag:

    """
    Создайте новый DAG, состоящий из одного BashOperator. Этот оператор должен  использовать шаблонизированную команду 
    следующего вида: "Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts и затем распечатать 
    значение run_id". Здесь ts и run_id - это шаблонные переменные 
    (вспомните, как в лекции подставляли шаблонные переменные).

    NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
    """

    templated_command = dedent(
        """
        {% for i in {0...4} %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    task = BashOperator(
        task_id='templated',
        bash_command=templated_command
    )

task
