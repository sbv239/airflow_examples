"""

Напишите DAG, состоящий из одного PythonOperator.
Этот оператор должен, используя подключение с
conn_id="startml_feed", найти пользователя, который
поставил больше всего лайков, и вернуть пару
(user_id, количество_лайков). Эта пара, кстати,
сохранится в XCom.

"""

from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator


def db_conn():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action) likes
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY likes DESC
                LIMIT 1
                """
            )
            result = cursor.fetchall()
    return result


with DAG(
        'rakhimova_task10',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='DAG10 Rakhimova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['hehe'],
) as dag:

    t1 = PythonOperator(
        task_id='top_user',
        python_callable=db_conn,
    )

