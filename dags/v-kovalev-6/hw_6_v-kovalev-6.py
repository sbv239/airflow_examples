from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_6_v-kovalev-6',
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
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_6'],
) as dag:

    def task_number(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    for i in range(20):
        t1 = PythonOperator(
            task_id='python_operator_' + str(i),
            python_callable=task_number,
            op_kwargs={'task_number': i}
        )
