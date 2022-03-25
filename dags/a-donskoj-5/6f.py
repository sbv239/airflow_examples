from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_7_a-donskoj-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG in 6 step',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['task 6'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_'+str(i),
            bash_command=f"echo {i}"
        )

        t1.doc_md = dedent(
            """
        # BashOperator-TASK
        """
        )

    def func(num, ts, run_id, **kwargs):
        print(f"task number is: {num}")
        print(f"ts = {ts}")
        print(f"run_id = {run_id}")

    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=func,
            op_kwargs={'num': i}
        )

    t2.doc_md = dedent(
        """
    # PythonOperator-TASK
    """)

    t1 >> t2