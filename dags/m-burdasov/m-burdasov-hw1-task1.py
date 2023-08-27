"""
Напишите DAG, который будет содержать BashOperator и PythonOperator.
В функции PythonOperator примите аргумент ds и распечатайте его.
Можете распечатать дополнительно любое другое сообщение.

В BashOperator выполните команду pwd, которая выведет директорию,
где выполняется ваш код Airflow.
Результат может оказаться неожиданным, не пугайтесь - Airflow может
запускать ваши задачи на разных машинах или контейнерах
с разными настройками и путями по умолчанию.

Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator.
"""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'm-burdasov-hw11-t1',
    # Параметры по умолчанию для тасок
    default_args = {
        'depends_on_past': False,  # не ждать успешных запусков прошлых задач
        'email': ['airflow@example.com'],  # кому будут приходить отбойники при ошибках
        'email_on_failure': False,  # не писать при возникновении ошибок
        'email_on_retry': False,  # не писать при автоматическом перезапуске после ошибки
        'retries': 1,  # использовать 1 попытку перезапуска (после отметить как failed)
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='Task 1 DAG. Just execute pwd command.',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Не запускать за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать DAG-и
    tags=['hw11-1'],
) as dag:
    bash_task = BashOperator(
        task_id='print_work_dir',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    def print_ds(ds, **kwargs):
        print(f"Получено значение ds: {ds}")

    py_task = PythonOperator(
        task_id='return_ds',
        python_callable=print_ds
    )

    dag.doc_md = __doc__  # Забрать документацию из начала файла
    bash_task >> py_task  # граф будет выглядеть так: bash_task -> py_task
