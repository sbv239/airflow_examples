from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'hw_5_a-tselyh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step_5'],
) as dag:

    templated_command = dedent(
        """
    {% for i in range({NUMBER}) %}
        echo "{{ ts }}"
    {% endfor %}
    echo "{{ run_id }}"
    """
    )

    t1 = BashOperator(
    for i in range(10):
        os.environ['NUMBER'] = str(i)
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command="echo '{}'".format(os.environ['NUMBER']),
        )

    t1
