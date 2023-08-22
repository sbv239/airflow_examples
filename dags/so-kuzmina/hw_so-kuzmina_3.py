from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
)

def print_task_number(task_number):
    print(f"task number is: {task_number}")

with dag:
    bash_tasks = []
    python_tasks = []

    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id=f'bash_task_{i}',
                bash_command=f'echo {i}',
                dag=dag,
            )
            bash_tasks.append(task)
            if i > 0:
                bash_tasks[i - 1] >> task  # Устанавливаем зависимость от предыдущей bash-задачи
        else:
            task = PythonOperator(
                task_id=f'python_task_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
                dag=dag,
            )
            python_tasks.append(task)
            if i > 10:
                python_tasks[i - 1] >> task  # Устанавливаем зависимость от предыдущей python-задачи
