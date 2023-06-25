from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_2_a-samofalov',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='A simple tutorial DAG june 2023',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_7_a-samofalov'],
) as dag:

    def print_context(ts, run_id, task_number):
        print(ts)
        print(run_id)
        print(task_number)
        return 'Whatever you return gets printed in the logs'

    for i in range(30):
        t1 = PythonOperator(
            task_id='PRINT_PythonOp_Samofalov_' + str(i),  # нужен task_id, как и всем операторам
            python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
            op_kwargs={'task_number': i}
        )
