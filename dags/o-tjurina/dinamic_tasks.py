"""
Test documentation
"""
from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator

with DAG(
        dag_id='hw_3_o-tjurina',
        # Параметры по умолчанию для тасок
        default_args={
            # Если прошлые запуски упали, надо ли ждать их успеха
            'depends_on_past': False,
            # Кому писать при провале
            'email': ['olesia.tiurina@outlook.com'],
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
        description='A lesson11 task 3 DAG',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2023, 1, 24),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['example'],
) as dag:
    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_task_{i}_by_BashOperator',  # id, будет отображаться в интерфейсе
            bash_command=f"echo task {i}",  # какую bash команду выполнить в этом таске
        )


    def print_context(task_number):
        """Пример PythonOperator"""
        print(f'task number is: {task_number}')
        return 'Whatever'


    for i in range(20):
        v_task_number = 10 + i
        t2 = PythonOperator(
            task_id=f'print_task_{v_task_number}_by_PythonOperator',  # нужен task_id, как и всем операторам
            python_callable=print_context,
            # передаем в аргумент с названием in_task_number значение task_number
            op_kwargs={'task_number': v_task_number}
        )

    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
