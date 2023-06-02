from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor


def get_user():
    creds = BaseHook.get_connection('startml_feed')
    with psycopg2.connect(
      f"postgresql://{creds.login}:{creds.password}"
      f"@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
      with conn.cursor(cursor_factory=RealDictCursor) as cursor:
          cursor.execute(
              f"""
                SELECT  user_id, count(user_id)
                FROM "feed_action"   
                where action = 'like'
                Group BY user_id
                Order by count(user_id) desc
                limit 1
                 """
          )
          answer = cursor.fetchone()
    return answer



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_dmi-rusin_11',
        start_date=datetime(2023, 6, 1),
        schedule_interval=timedelta(minutes=5),
        max_active_runs=2,
        default_args=default_args,
        catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id='task_11',
        python_callable=get_user
    )