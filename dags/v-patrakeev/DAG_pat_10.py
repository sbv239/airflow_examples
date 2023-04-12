from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor



from airflow.providers.postgres.operators.postgres import PostgresHook
def show_like():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
      with conn.cursor() as cursor:
          cursor.execute("""
              SELECT user_id, COUNT(action)
              FROM feed_action
              WHERE action = 'like'
              GROUP BY user_id
              ORDER BY COUNT(action) DESC
              LIMIT 1
              """)
          return cursor.fetchone()
with DAG(
    'HW_11_v-patrakeev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now(),
) as dag:
    t1 = PythonOperator(
        task_id='user_like',
        python_callable=show_like
    )

    t1
