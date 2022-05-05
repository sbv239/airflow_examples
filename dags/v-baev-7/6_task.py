from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    "hw_6_v-baev-7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='More args in DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=5, day=3),
    catchup=False,
    tags=['hw_6'],
) as dag:
        for i in range(10):
                task_b = BashOperator(
                task_id='print'+str(i),
                bash_command=f"echo {i}"
        )


def print_context(task_num, ts, run_id):
        print(f'task number is: {task_num}')
        print(ts)
        print(run_id)


for task_n in range(20):
        task_p = PythonOperator(
                task_id='task_number' + str(i),
                python_callable=print_context,
                op_kwargs={'task_num': i},
        )
task_b >> task_p