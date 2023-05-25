from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator


def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

with DAG(
    'hw_i-loskutov_2'
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

    description='task02',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 05, 26),
    catchup=False

) as dag:

    t1 = BashOperator(
    task_id = 'print_pwd_task02',
    bash_command = 'pwd'
    )
    t2 = PythonOperator(
    task_id = 'get_date__task02',
    python_callable = print_context,
    )
    t1 >> t2




