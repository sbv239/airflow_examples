from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'hw_5_d-nikolaev',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )
    task = BashOperator(
            task_id=f'hw_d-nikolaev-20_1',
            bash_command=templated_command)

    task.doc_md = dedent(f'''
            # Задача по выводу `ts` и `run_id`

            ''')



