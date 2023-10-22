from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_k-silina_3',
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
    description='hw3_k_silina',
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
) as dag:
    for i in range(10):
        task_bash = BashOperator(
            task_id = f'print_{i}',
            bash_command = f"echo {i}",
        )
    def print_context(i, **kwargs):
        task_number = i + 10
        print(f"task number is: {task_number}")
        return 'Whatever you return gets printed in the logs'

    for i in range(20):
        task_python = PythonOperator(
            task_id='print_number_' + str(i),
            python_callable=print_context,
            op_kwargs={'i': i},
        )
        # настраиваем зависимости между задачами
        task_bash >> task_python
