from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task_4_se-sheshkus-11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Less_11_task_4_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_4_se-sheshkus-11'],
) as dag:



    for i in range(10):
        t1 = BashOperator(
            task_id='bash_operator_' + str(i),
            bash_command=f"echo {i}",
        )

        t1.doc_md = dedent(
            f"""\
        # Task Documentation
        This **task** *execute* bash_command = `echo {i}`
        """
        )



    cmd_template = dedent(
        '''
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        '''
    )

    t1 = BashOperator(
        task_id='cmd_template',
        bash_command=cmd_template
    )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    This **task** execute command print `ts` 5 times and print `run_id` using jinja
    """
    )
