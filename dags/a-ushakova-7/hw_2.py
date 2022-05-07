from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG('AUshakova_HW_2',
# Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
            # Кому писать при провале
        'email': ['airflow@example.com'],
            # А писать ли вообще при провале?
        'email_on_failure': False,
            # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
            # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
            # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
            # Описание DAG (не тасок, а самого DAG)
    description='2 homework',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 6),
    catchup=False,
    tags=['hw_2'], ) as dag:

    templated_command = dedent(
        """
        f"echo {i}"
    """

    for i in range(10):
        task_id = BashOperator(task_id='templated_command_' + str(i),  # id, будет отображаться в интерфейсе
                      bash_command = templated_command,  # какую bash команду выполнить в этом таске
                                        )
        task_id

    def print_number(number):
        print("task number is: {number}".format(number))

    for i in range(20):
    # Каждый таск будет спать некое количество секунд
        task = PythonOperator(
            task_id='number_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable = print_number,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'number': int(i)},
            )
    # настраиваем зависимости между задачами
    # run_this - это некий таск, объявленный ранее (в этом примере не объявлен)
        task_id >> task

