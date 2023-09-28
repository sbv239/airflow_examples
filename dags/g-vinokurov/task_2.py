from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_g-vinokurov_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
        description='DAG in task_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 28),
        catchup=False,
) as dag:

    for i in range(9):
        operator_1 = BashOperator(
            task_id='Bash_operator2',
            bash_command=f"Count BashOperator: {i}",
            dag=dag,
        )

    def count_task(**kwargs):
        print("task number is: {**kwargs}")

    for i in range(19):
        operator_2 = PythonOperator(
            task_id='Python_operator2',
            python_callable=count_task,
            op_kwargs={'**kwargs': i}
        )

    operator_1 >> operator_2