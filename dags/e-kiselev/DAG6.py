from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_6_e-kiselev',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Ex. 6.',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 16),
    catchup=False,
    tags=['example'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='bash_' + str(i),
            env={"NUMBER": i},
            bash_command="echo $NUMBER"
        )

    def task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_task_number_{i}',
            python_callable=task_number,
            op_kwargs={
                'task_number': i}
        )

    t1 >> t2