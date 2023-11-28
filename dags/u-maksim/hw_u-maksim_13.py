from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.models import Variable


with DAG(
    'hw_u-maksim_13',
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
    description='DAG 13',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:
    
    def choice():
        if Variable.get("is_startml") == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    def print_true():
        print("StartML is a starter course for ambitious people")

    def print_false():
        print("Not a startML course, sorry")

        
    t1 = BranchPythonOperator(
        task_id='choice',
        python_callable=choice   
)

    t2 = PythonOperator(
    task_id="startml_desc",  # нужен task_id, как и всем операторам
    python_callable=print_true,  # свойственен только для PythonOperator - передаем саму функцию
)
    t3 = PythonOperator(
    task_id="not_startml_desc",  # нужен task_id, как и всем операторам
    python_callable=print_false,  # свойственен только для PythonOperator - передаем саму функцию
)
    # А вот так в Airflow указывается последовательность задач
    t1 >> [t2, t3]