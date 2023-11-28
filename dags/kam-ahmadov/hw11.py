from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import RealDictCursor
from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_top_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            top_user = """
            SELECT user_id, COUNT(action) AS count
            FROM "feed_action"
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 1
            """
            cursor.execute(top_user)
            result = cursor.fetchone()
    return result

def save_top_user(ti):
    value = ti.xcom_pull(task_ids="get_top_user")
    print(f"Top user: {value}")
    return value

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_kamilahmadov_10',
    default_args=default_args,
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_10"]
) as dag:
    
    get_top_user_task = PythonOperator(
        task_id="get_top_user",
        python_callable=get_top_user,
    )

    save_xcom_task = PythonOperator(
        task_id="top_user_save",
        python_callable=save_top_user,
    )

    get_top_user_task >> save_xcom_task
