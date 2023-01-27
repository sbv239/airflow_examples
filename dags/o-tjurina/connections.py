"""
Test documentation
"""
from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

with DAG(
        dag_id='hw_11_o-tjurina',
        # Параметры по умолчанию для тасок
        default_args={
            # Если прошлые запуски упали, надо ли ждать их успеха
            'depends_on_past': False,
            # Кому писать при провале
            'email': ['olesia.tiurina@outlook.com'],
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
        description='A lesson11 task 11 DAG',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2023, 1, 27),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['example'],
) as dag:
    def print_context():
        postgres = PostgresHook(postgres_conn_id="startml_feed", schema="public")
        with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                cursor.execute("""
                        select user_id, count(action)
                        from feed_action
                        where action = 'like'
                        group by user_id
                        order by 2 desc 
                    """)
                result = cursor.fetchone()
        print(result)
        return result


    get_user = PythonOperator(
        # provide context is for getting the TI (task instance ) parameters
        task_id='get_user_with_max_likes',
        python_callable=print_context,
    )

    # А вот так в Airflow указывается последовательность задач
    get_user
