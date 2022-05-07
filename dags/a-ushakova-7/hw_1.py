from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG('AUshakova_HW_1',
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
    description='First homework',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 6),
    catchup=False,
    tags=['hw_1'], ) as dag:

    t1 = BashOperator(task_id='print_pwd',  # id, будет отображаться в интерфейсе
                      bash_command='pwd',  # какую bash команду выполнить в этом таске
                                        )

    def print_context(ds):
    """Пример PythonOperator"""
    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)

    t2 = PythonOperator(
    task_id='print_the_context',  # нужен task_id, как и всем операторам
    python_callable=print_context,)  # свойственен только для PythonOperator - передаем саму функцию


    t1 >> t2
