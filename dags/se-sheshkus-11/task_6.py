from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task_6_se-sheshkus-11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Less_11_task_6_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_6_se-sheshkus-11'],
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
