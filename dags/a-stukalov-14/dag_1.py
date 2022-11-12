from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dag_1',
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
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval = timedelta(days = 2),

    start_date = datetime(2022,11,12),
    catchup=False,
    # теги, способ помечать даги
    tags=['dag_1_st']
) as dag:

t1 = BashOperator(
    task_id = 'pwd',
    bash_command = 'pwd'
)

def print_context(ds,**kwargs):
    print(kwargs)
    print(ds)

t2 = PythonOperator(
    task_id = 'ds',
    python_callable = print_context
)

t1 >> t2