from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


def find_user_with_most_likes():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            # Execute SQL query to find user with most likes
            cursor.execute("""
                SELECT user_id, COUNT(*) AS like_count
                FROM feed_action
                GROUP BY user_id
                ORDER BY like_count DESC
                LIMIT 1
            """)
            result = cursor.fetchone()

            # Create dictionary with user_id and like_count
            user_dict = {
                'user_id': result[0],
                'count': result[1]
            }

            return user_dict


with DAG(
    'hw_11_d-ivashkin-23',
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

    task = PythonOperator(
        task_id='find_user_with_most_likes',
        python_callable=find_user_with_most_likes
    )

task
