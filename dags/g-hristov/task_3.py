from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_g-hristov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:
    for i in range(10):
        taskbo = BashOperator(
            task_id='hw_g-hristov_3_BO'+str(i),
            bash_command=f"echo {i}",
        )

    def print_context(task_number):
        print(f"task number is: {task_number}")


    for i in range(20):
        taskpo = PythonOperator(
            task_id='hw_g-hristov_3_PO'+str(i),
            python_callable=print_context,
            op_kwargs={"task_number": i}
        )


    taskbo >> taskpo