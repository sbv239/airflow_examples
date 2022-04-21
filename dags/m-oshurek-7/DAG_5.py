from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


with DAG(
    # имя дага, которое отразиться на сервере airflow
    'DAG_5_oshurek',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание самого дага
    description='Задание1. Напишите DAG, который будет содержать BashOperator и PythonOperator.'
                ' В функции PythonOperator примите аргумент ds и распечатайте его.',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example_5_oshurek'],
) as dag:
    
    for i in range(5):
        task_bash = BashOperator(
            task_id = 'task_bash_' + str(i),
            bash_command = f"echo $NUMBER",
            env={'NUMBER': str(i)},
            )
                    
    task_bash
    
