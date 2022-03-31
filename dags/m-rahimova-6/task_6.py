from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator

with DAG(
    'rakhimova_task6',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG6 Rakhimova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['hehe'],
) as dag:

    def print_task_nubmer(ts, run_id, task_number, **kwargs):
        print(ts)
        print(run_id)


    for j in range(20):
        t_python = PythonOperator(
                task_id=f'python_task_{j}',
                python_callable=print_task_nubmer,
                op_kwargs={'task_number': j}
        )
