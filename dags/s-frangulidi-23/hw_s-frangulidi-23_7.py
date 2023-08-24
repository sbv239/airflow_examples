from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Определение аргументов DAG
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

# Создание объекта DAG
dag = DAG(
    'hw_s-frangulidi-23_6',
    # Описание DAG (не тасок, а самого DAG)
    description='task_3 DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    start_date=datetime(2023, 8, 24),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
)
# Функция для PythonOperator
def print_context(ts, run_id, task_number, **kwargs):
    print(f"Task number is: {task_number}")
    print(f"Execution date: {ts}")
    print(f"Run ID: {run_id}")

# Создание задач в цикле
for i in range(1, 31):
    if i <= 10:
        task = BashOperator(
            task_id=f'bash_task_{i}',
            bash_command=f'echo $NUMBER',
            env={'NUMBER': str(i)},  # Передача переменной окружения NUMBER
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'python_task_{i}',
            python_callable=print_context,
            op_kwargs={'task_number': i},
            provide_context=True,  # Передача контекста
            dag=dag,
        )

    task

task  # Добавляем задачу в DAG

# Порядок выполнения для всех задач
for i in range(1, 30):
    dag.tasks[i - 1] >> dag.tasks[i]