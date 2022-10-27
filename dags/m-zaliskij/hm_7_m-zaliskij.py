from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_7_m-zaliskij',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2022, 1, 1),
        catchup=False

) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f"echo{i}",
            bash_command=f"echo {i}"
        )


    def task_num(task_number, ts, run_id):
        print(f'task number is {task_number}')
        print(ts)
        print(run_id)


    for j in range(10, 30):
        t2 = PythonOperator(
            task_id=f'task_num{j}',
            python_callable=task_num,
            op_kwargs={'task_number': j}
        )

    t1.doc_md = dedent('''
    `t1`
    # *strange* **bold**
    ''')
