from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='First DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task3'],
) as dag:

    def task(task_number):
        print(f"task number is: {task_number}")

    for i in range(30):
        if i < 10:
            task_bash = BashOperator(
                task_id='bash_task_' + str(i),
                bash_command=f"echo {i}",
            )
        else:
            task_python = PythonOperator(
                task_id='python_task' + str(i),
                python_callable=task,
                op_kwargs={'task_number': i},
            )

    task_bash >> task_python
