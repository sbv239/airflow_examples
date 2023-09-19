"""
hw_n-knjazeva-24_3
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

def print_task_num(ts, run_id, **kwargs):
    print(f"task number is: ${kwargs['task_number']}")
    print(ts)
    print(run_id)
    return

with DAG(
        'hw_n-knjazeva-24_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 9, 18),
        schedule_interval=timedelta(days=1)
) as dag:

    for i in range(10):
        tasks_bo = BashOperator(
            task_id='bo' + str(i),
            bash_command="echo $NUMBER",
            env={'NUMBER': i}
        )

    for i in range(20):
        tasks_po = PythonOperator(
            task_id='po' + str(i),
            python_callable=print_task_num,
            op_kwargs={'task_number': i}
        )

    tasks_bo >> tasks_po

