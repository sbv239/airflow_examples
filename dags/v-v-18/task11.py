from airflow.providers.postgres.operators.postgres import PostgresHook
# from airflow.operators import PostgresOperator
from airflow import DAG
# from airflow.operators.python import task, PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta


with DAG(
    'dag_task11',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date = datetime(2023, 3, 23)
) as dag:
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            @task(task_id="get_active_user")
            def get_active_user():
                cursor.execute("""
                SELECT user_id, COUNT(action) AS "count"
                FROM "feed_action"
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(action) desc
                LIMIT 1
                """)
                result = cursor.fetchone()
                return result



    # get_active_user = PostgresOperator(
    #     task_id="get_active_user",
    #     postgres_conn_id="startml_feed",
    #     sql=
    #         """
    #         SELECT user_id, COUNT(action) AS "count"
    #         FROM "feed_action"
    #         WHERE action = 'like'
    #         GROUP BY user_id
    #         ORDER BY COUNT(action) desc
    #         LIMIT 1
    #         """,
    #     )