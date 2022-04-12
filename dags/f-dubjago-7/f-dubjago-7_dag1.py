# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
        'f-dubjago-7_dag1',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        tags=['task1']
) as dag:
    t = BashOperator(
        task_id="give_dir",
        bash_command="pwd ",  # обратите внимание на пробел в конце!
    )


    def print_context(ds, **kwargs):
        print(ds)


    run_this = PythonOperator(
        task_id='print_smth',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t >> run_this
