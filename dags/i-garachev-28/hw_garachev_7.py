from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_garachev_6_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_6'],
) as dag:

    ts = "{{ ts }}"
    run_id = "{{ run_id }}"
    def print_context(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f"hw_3_garachev_{i}_bash",
                bash_command=f"echo {i}"
            )
            t1
        else:
            t2 = PythonOperator(
                task_id=f'hw_3_garachev_{i}_python',
                python_callable=print_context ,
                op_kwargs = {"task_number": i}
            )
            t2