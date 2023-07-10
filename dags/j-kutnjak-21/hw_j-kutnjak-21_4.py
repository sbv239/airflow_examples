from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent


default_args = {'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                }

with DAG(
    'hw_j-kutnjak-21_3',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 30),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='j-kutnjak-21_task3-1_' + str(i),
            bash_command=f"echo {i}",
        )
        t1.doc_md = dedent("""
                        #### Task Documentation
                        Some `code`.
                        Some *text*.
                        Some **text**.
                        # Some text.
                    """)

    for i in range(20):
        def print_number(task_number = {i}):
            print(f"task number is: {task_number}")

        t2 = PythonOperator(
            task_id='print_number_' + str(i),
            python_callable=print_number,
            op_kwargs={'id': i}
        )
        t2.doc_md = dedent("""
                #### Task Documentation
                Some `code`.
                Some *text*.
                Some **text**.
                # Some text.
            """)
