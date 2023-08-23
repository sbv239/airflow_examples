from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_i(task_number, **kwargs):
    print(f"task number is: {task_number}")

with DAG(
    'hw_so-kuzmina_6',  # Обновляем название DAG
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
    # Создаем глобальную переменную
    dag_env = {'NUMBER': ''}

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'bash_task_{i}',
                bash_command=f'echo $NUMBER',  # Используем переменную окружения
                env=dag_env  # Указываем словарь с глобальной переменной
            )
            # Задаем значение глобальной переменной для текущей задачи
            dag_env['NUMBER'] = str(i)
        else:
            t2 = PythonOperator(
                task_id=f'python_task_{i}',
                python_callable=print_i,
                op_kwargs={'task_number': f'{i}'}
            )
