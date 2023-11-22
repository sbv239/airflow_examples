"""
Task-5: работа с шаблонизатором Jinja
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
    'hw_i-vafin_5',
    default_args={},
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 22),
    tags=['hw_i-vafin'],
) as dag:
    dag.doc_md = __doc__

    templated_bash_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id='bash_templated_task',
        bash_command=templated_bash_command
    )
    t1.doc_md = """
    Документация для задачи
    """

    t1
