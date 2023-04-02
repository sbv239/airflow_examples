from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_3_a-korenevich-1',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    # Описание DAG (не тасок, а самого DAG)
    description='Task #3',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 4, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:

    for i in range(10):
        # Каждый таск будет выводить его номер
        task = BashOperator(
            task_id='print_bash_task_id_' + str(i),  # id, будет отображаться в интерфейсе
            bash_command=f"echo {i}",  # какую bash команду выполнить в этом таске
        )
    
    def print_task_id(task_number):
        """Вывести task_number"""
        print("task number is: {task_number}")

    for i in range(10, 30):
        # Каждый таск будет выводить его номер
        task = PythonOperator(
            task_id='print_python_task_id_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_task_id,
            # передаем в аргумент с названием task_number номер таски i
            op_kwargs={'task_number': i},
        )
