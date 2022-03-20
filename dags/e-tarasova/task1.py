from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tarasova_task1',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG for task 1 Tarasova E',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 19),
    catchup=False,
    tags=['task1']

,
) as dag:

    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'

    )

    t1.doc_md = dedent(
        """
        В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow
        """
    )

    def print_ds(ds):
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    t1 >> t2
