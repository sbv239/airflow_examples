from airflow.hooks.base_hook import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

connection = BaseHook.get_connection("startml_feed")
conn_password = connection.password
conn_login = connection.login



def get_base():
  with psycopg2.connect(
    f"postgresql://{connection.login}:{connection.password}"
    f"@{connection.host}:{connection.port}/{connection.schema}"
  ) as conn:
    with conn.cursor(cursor_factory= RealDictCursor) as cursor:
      cursor.execute(
        """
        SELECT user_id, COUNT(action)
        FROM feed_action
        WHERE action = 'like'
        GROUP by user_id
        ORDER BY COUNT(action) desc
        """
      )
      return cursor.fetchone()

with DAG(
          'hw_11_ex_11-n-jazvinskij',
          default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
          },
          description='hw_11_ex_11',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2023, 10, 21),
          catchup=False,
) as dag:

  t1 = PythonOperator(
    task_id = 'get_query_response',
    python_callable = get_base
  )
  t1