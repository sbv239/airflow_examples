from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


with DAG(
    # имя дага, которое отразиться на сервере airflow
    'DAG_8_oshurek',
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
    tags=['example_8_oshurek'],
) as dag:
    def pusher(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )


    def puller(ti):
        xcom_value = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='pusher'
        )
        print(xcom_value)

    t1 = PythonOperator(
        task_id='pusher',
        python_callable=pusher,
    )

    t2 = PythonOperator(
        task_id='puller',
        python_callable=puller,
    )

    t1 >> t2
    
