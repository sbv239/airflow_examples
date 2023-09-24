from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds, **kwargs):
    print(ds)


with DAG(
    "hw_a-ulyanov_2",
    # Параметры по умолчанию для тасок
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description="hw_2 DAG",
    start_date=datetime(2023, 9, 23),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    t1 = BashOperator(
        task_id="print_dir",  # id, будет отображаться в интерфейсе
        bash_command="pwd",  # какую bash команду выполнить в этом таске
    )

    t2 = PythonOperator(
        task_id="print_the_context",  # нужен task_id, как и всем операторам
        python_callable=print_ds,  # свойственен только для PythonOperator - передаем саму функцию
    )

t1 >> t2
