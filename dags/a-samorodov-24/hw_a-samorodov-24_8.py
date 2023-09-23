from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG (
        'hw_8_a-samorodov-24',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:

    def print_context(task_numbers, ts, run_id, **kwargs):
        print(f"task number is: {task_numbers}, {ts}, {run_id}")

    for i in range(30):
        if i < 10:
            task = BashOperator(
            task_id=f"print_{i}",
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
            )
        else:
            run_this = PythonOperator(
                task_id=f'print_the_{i}',  # нужен task_id, как и всем операторам
                python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
                op_kwargs={'task_numbers': i}
            )





