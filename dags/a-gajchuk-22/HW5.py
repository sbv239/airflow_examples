from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent

with DAG('hw_5_a-gajchuk-22',
         default_args = {
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
                    },
         start_date = datetime(2023,7,21),
         tags = ['agaychuk5']) as dag:

    
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """)


    t1 = BashOperator(task_id = 'print_ts_and_run_id',
                      bash_command = templated_command)


    t1
    
