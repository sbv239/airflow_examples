from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.hooks.base import BaseHook
import psycopg2

def load():
    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
      f"postgresql://{creds.login}:{creds.password}"
      f"@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
      with conn.cursor() as cursor:
          cursor.execute(
          """
          SELECT FEED.user_id, COUNT(FEED.action)
          FROM FEED
          WHERE FEED.action= 'like'
          GROUP BY FEED.user_id
          ORDER BY COUNT(FEED.action) DESC
          LIMIT 1;
          """
          );
          s=cursor.fetchone()
          print(s)
    return s
with DAG(
    'norgello-10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='first task in lesson №11',
    schedule_interval=timedelta(days=3650),
    start_date=datetime(2022, 10, 20),
    catchup=False
) as dag:
    m1=PythonOperator(
        task_id='comm_pwd',
        python_callable=load)
