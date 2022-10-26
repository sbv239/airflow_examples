from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_3_m-zaliskij',
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
            bash_command=f"echo{i}"
        )


    def task_number(task_num):
        print(f'task number is {task_num}')


    for j in range(10, 30):
        t2 = PythonOperator(
            task_id=f'task_num{j}',
            python_callable=task_number,
            op_kwargs={'task_num': j}
        )
