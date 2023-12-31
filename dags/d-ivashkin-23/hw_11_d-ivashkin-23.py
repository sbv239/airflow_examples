from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresHook


with DAG(
    'hw_d-ivashkin-23_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 11-th step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

    """
    Напишите DAG, состоящий из одного PythonOperator.
    Этот оператор должен, используя подключение с conn_id="startml_feed",
    найти пользователя, который поставил больше всего лайков, и вернуть словарь
    {'user_id': <идентификатор>, 'count': <количество лайков>}. Эти значения, кстати, сохранятся в XCom.
    """


    def most_likes_user():
        from psycopg2.extras import RealDictCursor
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    select user_id, count(*) as count
                    from feed_action
                    where action = 'like'
                    group by user_id
                    order by count desc
                    limit 1
                """)
                return cursor.fetchone()


    task = PythonOperator(
        task_id='most_likes_user',
        python_callable=most_likes_user
    )

task
