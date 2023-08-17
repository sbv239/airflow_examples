from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    "hw_m-golovaneva_task2",

    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='my DAG for 2d task Lecture 11',

    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),

    # С какой даты начать запускать DAG (Каждый DAG "видит" свою "дату запуска" - это когда он предположительно должен был запуститься.
    # Не всегда совпадает с датой на компьютере):
    start_date=datetime(2022, 1, 1),

    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,

    # теги, способ помечать даги
    tags=['task2_L11']
) as dag:

    task1 = BashOperator(
        task_id="print_directory",
        bash_command="pwd"
    )

    def print_ds(ds, **kwargs):
        print(ds)
        print(kwargs)
        return "We print ds and anything else that comes"

    task2 = PythonOperator(
        task_id="print_ds_and_more",
        python_callable=print_ds)

    # tasks sequence
    task1 >> task2


