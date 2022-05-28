"""
Task 10: https://lab.karpov.courses/learning/84/module/1049/lesson/10040/29383/139295/
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

with DAG(
    'hw_10_s.zlenko-7',
    default_args = default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    def get_user():
        from airflow.providers.postgres.operators.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor

        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    select user_id,count(post_id)
                    from feed_action 
                    where action like 'like'
                    group by user_id
                    order by count(post_id) desc
                    limit 1
                    """
                )
                result = cursor.fetchall()
        return result

    t1 = PythonOperator(
        task_id='get_user',
        python_callable=get_user
    )

    t1

