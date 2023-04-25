from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        dag_id='hw_szaripova_2',
        default_args=default_args,
        description='DAG for Step 2',
        start_date=datetime(2023, 4, 25)
) as dag:

    bash_tasks = []
    for i in range(10):
        bash_tasks.append(
            BashOperator(
                task_id='hw_szaripova_2_bash',
                bash_command=f'echo {i}'
            )
        )

    def print_task_number(task_number):
        print(ds)
        print('some text')


    t2 = PythonOperator(
        task_id='hw_szaripova_2_python',
        python_callable=print_date
    )

    t1 >> t2
