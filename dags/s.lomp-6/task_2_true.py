from airflow import DAG
from datetime import datetime, timedelta



from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_context(task_number, **kwargs):
    """Пример PythonOperator"""
    # Через синтаксис **kwargs можно получить словарь
    # с настройками Airflow. Значения оттуда могут пригодиться.
    # Пока нам не нужно

    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    print(f"task number is: {task_number}")
    return 'Whatever you return gets printed in the logs'


with DAG(
    'slomp_task_2',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(10):
        # Каждый таск будет спать некое количество секунд
        task_bash = BashOperator(
            task_id=f"task {i} slompba" ,  # в id можно делать все, что разрешают строки в python
            bash_command=f"echo {i}"  # какую bash команду выполнить в этом таске
        )

    for i in range(20):
        task_py = PythonOperator(
        task_id = f"task {i} slomppy",  # нужен task_id, как и всем операторам
        python_callable = print_context,  # свойственен только для PythonOperator - передаем саму функцию
        # передаем в аргумент с названием random_base значение float(i) / 10
        op_kwargs={'task_number': str},
        )


    task_bash >> task_py
