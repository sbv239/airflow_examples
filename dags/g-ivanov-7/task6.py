from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'G-Ivanov-task6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task in the lesson on Airflow',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i}',
            bash_command=f'echo $NUMBER',
            env={'NUMBER': i},
            dag=dag,
        )


    def print_task_num(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number}. ts - {ts}. run_id - {run_id}")


    for i in range(20):
        t2 = PythonOperator(
            task_id=f'task_num_{i}',
            python_callable=print_task_num,
            op_kwargs={"task_number": i},
        )

    t1 >> t2
