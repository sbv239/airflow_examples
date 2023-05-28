from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_an-kravchuk_4',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['task_4'],
) as dag:

    for i in range(10):
        bash_task = BashOperator(
            task_id=f'Bash_task_{i}',
            bash_command=f'echo {i}'
        )
        bash_task

        bash_task.doc_md = dedent(
        """
        # Task documentation

        `code`, **bold text**, *cursive text*

        another text

        ## header 2

        **text*
        """
        )
