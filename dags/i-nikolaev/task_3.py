from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
with DAG(
    # Название
    'I_nikolaev_task_3',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:
    def get_num(random_base):
        print(f"task number is: {random_base}")

    # Генерируем таски в цикле - так тоже можно
    for i in range(10):
        # Каждый таск будет спать некое количество секунд
        taskBash = BashOperator(
            task_id='command_' + str(i),  # в id можно делать все, что разрешают строки в python
            bash_command = f"echo {i}"
        )
        for i in range(20):
            # Каждый таск будет спать некое количество секунд
            taskPython = PythonOperator(
                task_id='command_' + str(i),  # в id можно делать все, что разрешают строки в python
                python_callable=get_num,
                # передаем в аргумент с названием random_base значение float(i) / 10
                op_kwargs={'random_base': float(i) / 10},
            )
        # настраиваем зависимости между задачами
        taskBash >> taskPython