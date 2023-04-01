from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta




with DAG(
     'lebedev_zad1',
     # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description="Zad 1 DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
    ) as dag:

    t1 = BashOperator(
        task_id='pwd_task',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    def print_context(ds, **kwargs):
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='print_the_context',  # нужен task_id, как и всем операторам
        python_callable=print_context  # свойственен только для PythonOperator - передаем саму функцию
        )
    t1 >> t2
    
    