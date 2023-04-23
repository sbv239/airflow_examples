from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from textwrap import dedent

with DAG(
        'hw_7_mi-ponomarev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_re            try': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task_7',
    schedule_interval=timedelta(days=365),
    start_date=datetime(2023, 4, 22),
    catchup=False,
    tags=['task_7']
) as dag:
        for i in range(10):
                task_1 = BashOperator(
                        task_id=f'echo' + str(i),
                        bash_command="echo NUMBER",
                        env={"NUMBER": i},
                )

        def print_task_number(task_number, ts, run_id):
                print(f"task number is: {task_number}", ts, run_id, sep='\n')

        for i in range(20):
                task_2 = PythonOperator(
                        task_id=f"print" + str(i),
                        python_callable=print_task_number,
                        op_kwargs={'task_number': i}
                )