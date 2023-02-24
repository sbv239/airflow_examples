from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'task2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:
    # t1, t2 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='print_pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )


    def print_ds(ds, **kwargs):
        print(ds)


    t2 = PythonOperator(
        task_id='print_ds',  # нужен task_id, как и всем операторам
        python_callable=print_ds,  # свойственен только для PythonOperator - передаем саму функцию
    )

    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
