from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
        'hw_1_r-romanov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),

        catchup=False,
        # теги, способ помечать даги
        tags=['rm_romanov'],
) as dag:
        templated_command = dedent(
            """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """)
        ts = '{{ts}}'
        run_id = '{{run_id}}'
        task = BashOperator(
            task_id='id_task',
            bash_command=templated_command,
            env = {'ts':ts, 'run_id':run_id}
        )
        task.doc_md = dedent(
            """\
        # Task BashOperator

        `You can` document your *task using* the **attributes** `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        """
        )
