from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


def get_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn(cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cursor:
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
    'hw_e-zastrogina_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 15)
) as dag:
    t1 = PythonOperator(
        task_id='get_user_likes',
        python_callable=get_user,
        provide_context=True
    )
