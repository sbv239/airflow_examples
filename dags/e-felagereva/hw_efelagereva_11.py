from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

def most_likes():
  postgres = PostgresHook(postgres_conn_id="startml_feed")
  with postgres.get_conn() as conn:
    with conn.cursor(cursor_factory = RealDictCursor) as cursor:


        query = """
      SELECT user_id, COUNT(*)
      FROM feed_action
      WHERE action = 'like'
      GROUP BY user_id
      ORDER BY COUNT(*) DESC
      LIMIT 1
      """
        cursor.execute(query)
        result = cursor.fetchone()
    return result


with DAG(
    'hw_efelagereva_11',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'a dag with connection',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 21)
) as dag:
    t1 = PythonOperator(
        task_id = 'most_likes',
        python_callable = most_likes
    )
