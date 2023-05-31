"""
Напишите DAG, состоящий из одного PythonOperator.
Этот оператор должен, используя подключение с conn_id="startml_feed", done
найти пользователя, который поставил больше всего лайков, done
и вернуть словарь {'user_id': <идентификатор>, 'count': <количество лайков>}. done
Эти значения, кстати, сохранятся в XCom.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from psycopg2.extras import RealDictCursor

def most_likes():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    postgres = PostgresHook(postgres_conn_id = "startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor: #RealDictCursor нужен, чтобы вернуть дикты по условию
            cursor.execute("""
            SELECT user_id, COUNT(*) as likes_count
            FROM feed_action
            GROUP BY user_id
            ORDER BY likes_count DESC
            LIMIT 1
            """)
            result = cursor.fetchone()
        return result

with DAG(
    'hw_11_ko-popov',
    default_args={
        'depends_on_past': False,
        'email': {'mdkonstantinp@gmail.com'},
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hw_11_ko-popov dag',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 5, 29),
    catchup=False,
    tags = ['hw_11_ko-popov'],
) as dag:
    t1 = PythonOperator(
        task_id = "most_likes",
        python_callable = most_likes
    )



