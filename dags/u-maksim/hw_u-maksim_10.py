from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_u-maksim_10',
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
    description='A simple tutorial DAG',
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

    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    def push_x(ti):
        ti.xcom_push(
            key='push_x'
        )
        return "Airflow tracks everything"
    
        
        
    t1 = PythonOperator(
    task_id='push_x',  # нужен task_id, как и всем операторам
    python_callable=push_x,  # свойственен только для PythonOperator - передаем саму функцию
)
    def pull_x(ti):
        data_x = ti.xcom_pull(
        key='return_value',
        task_ids='push_x'
        )
        print(data_x)
        

    t2 = PythonOperator(
    task_id='pull_x',  # нужен task_id, как и всем операторам
    python_callable=pull_x,  # свойственен только для PythonOperator - передаем саму функцию
)
    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
    # будет выглядеть вот так
    #  t1 -> t2
    #     