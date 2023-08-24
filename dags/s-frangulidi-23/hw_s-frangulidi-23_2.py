from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Аргументы для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 23),
    'retries': 1,
}

# Создайте объект DAG
dag = DAG(
    'hw_s-frangulidi-23_2',
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
    description='task_2 DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 8, 24),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
)

# Определение функцию для PythonOperator
def print_context(ds, **kwargs):
    print("Executing PythonOperator")
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

# Определение операторов
t1 = BashOperator(
    task_id='run_bash_script',
    bash_command='pwd',
    dag=dag,
)

run_this = PythonOperator(
    task_id='run_python_function',
    python_callable=print_context,
    op_args=['{{ ds }}'],  # Передача аргумента ds
    dag=dag,
)

# Определение порядка выполнения задач
t1 >> run_this