from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
def get_max_like():
  postgres = PostgresHook(postgres_conn_id="startml_feed")
  with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
    with conn.cursor() as cursor:
      cursor.execute(
      '''
      SELECT COUNT(action), user_id
      FROM feed_action
      WHERE action = 'like'
      GROUP BY user_id
      ORDER BY COUNT(action) desc
      LIMIT 1     
      '''
    )
      res = cursor.fetchone()
      return res



with DAG(
          'a-klabukov_hw_11',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
          },
          description='A simple tutorial DAG',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2022, 1, 1),
          catchup=False,
          tags=['example'],
) as dag:


  task_for_get = PythonOperator(
    task_id = 'task_id_for_get_max',
    python_callable = get_max_like,

  )

