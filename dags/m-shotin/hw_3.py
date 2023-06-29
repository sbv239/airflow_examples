from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'hw_m-shotin_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False, 'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_2 DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 28),
        catchup=False,
        tags=['example'],
) as dag:
    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id='print_echo' + str(i),
                bash_command=f"echo {i}",
            )
        else:
            def fn(task_number):
                print(f"task number is: {task_number}")

            t2 = PythonOperator(
                task_id='task_namber' + str(i),
                python_callable=fn,
                op_kwargs={'task_number': i}
            )
    t1 >> t2
