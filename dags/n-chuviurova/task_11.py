from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_user():
    """
    оператор должен, используя подключение с conn_id="startml_feed",
    найти пользователя, который поставил больше всего лайков,
    и вернуть словарь {'user_id': <идентификатор>, 'count': <количество лайков>}
    """

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT user_id, COUNT(action)
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY COUNT(action) DESC
            """)
            return cursor.fetchone()


with DAG(
        "hw_11_n-chuviurova",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="Connections",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 15),
        catchup=False,
        tags=["task_11"],
) as dag:

    t1 = PythonOperator(
        task_id="hw_n-chuviurova_1",
        python_callable=get_user,
    )

    t1
