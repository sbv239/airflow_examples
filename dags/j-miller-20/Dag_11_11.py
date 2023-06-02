from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import psycopg2
#найти пользователя, который поставил больше всего лайков,
# и вернуть словарь {'user_id': <идентификатор>, 'count': <количество лайков>}

def find_top_likes_user():
    from psycopg2.extras import RealDictCursor
    pg_hook = PostgresHook(postgres_conn_id ="startml_feed")
    with pg_hook.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                select user_id, count(*) as count
                from feed_action
                where action = 'like'
                group by user_id
                order by count desc
                limit 1
            """)
            result = cursor.fetchone()

        return result


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_j-miller_11',
        description='A 11 simple tutorial DAG wtih Xcom',  # Описание DAG (не тасок, а самого DAG)
        schedule_interval=timedelta(days=1),  # Как часто запускать DAG
        start_date=datetime(2023, 6, 1),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        tags=['SimJul_11'],  # теги, способ помечать даги
) as dag:

    t1 = PythonOperator(
        task_id='super_like_user',
        python_callable=find_top_likes_user,
        op_kwargs={'postgres_conn_id': 'startml_feed'},
        provide_context=True
    )


    t1
