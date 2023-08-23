from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_context(task_number, ts, run_id, **kwargs):
    print(f"task number is: {task_number}")
    print(f"ts is: {ts}")
    print(f"run_id is: {run_id}")

with DAG(
    'hw_so-kuzmina_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='dag with for',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 20)
) as dag:
    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'bash_task_{i}',
                bash_command=f'export NUMBER={i} && echo $NUMBER',
                env={'NUMBER': ''}  # Создаем переменную окружения для BashOperator
            )
            dag_env = {'task_number': i}  # Глобальный словарь для передачи в kwargs
            t2 = PythonOperator(
                task_id=f'python_task_{i}',
                python_callable=print_context,
                op_kwargs=dag_env,  # Передаем словарь с task_number в kwargs
                provide_context=True
            )
            t1 >> t2

