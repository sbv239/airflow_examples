from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_user_with_likes():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
              SELECT user_id, COUNT(*)
              FROM feed_action
              WHERE action = 'like'
              GROUP BY user_id
              ORDER BY COUNT(*) DESC 
              """)
            return cursor.fetchone()


with DAG('hw-b-shramko-10',
         default_args={
             'depends_on_past': False,
             'email': ['airflow@example.com'],
             'email_on_failure': False,
             'email_on_retry': False,
             'retries': 1,
             'retry_delay': timedelta(minutes=5),
         },
         start_date=datetime(2022, 1, 1),
         catchup=False
         ) as dag:
    t1 = PythonOperator(
        task_id='get_top_user',
        python_callable=get_user_with_likes
    )
