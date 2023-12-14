from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'hw_k-sokolov_3',
        default_args=default_args,
        description='simple dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:

    t1 = DummyOperator(task_id='start_dag')
    t2 = DummyOperator(task_id='middle_dag')
    t3 = DummyOperator(task_id="end_dag")

    for i in range(10):
        first_type_dags = BashOperator(
            task_id=f'bash_task_{i+1}',
            bash_command=f"echo {i}"
        )


    def task_number_func(op_kwargs):
        print(op_kwargs)

    for j in range(20):
        second_type_dags = PythonOperator(
            task_id=f'python_task_{j+1}',
            python_callable=task_number_func,
            op_kwargs=j
        )


    t1 >> first_type_dags >> t2 >> second_type_dags >> t3
