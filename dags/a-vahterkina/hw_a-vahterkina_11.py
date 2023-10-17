from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def func():
    from airflow.hooks.base import BaseHook
    import psycopg2
    from psycopg2.extras import RealDictCursor

    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}",
            cursor_factory=RealDictCursor

    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """select user_id, count(action)
                   from feed_action
                   where action = 'like'
                   group by user_id
                   order by count(action) desc
                   limit 1
                """
            )
            result = cursor.fetchone()
            return result


with DAG(
        'hw_11_a-vahterkina',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_a-vahterkina_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 17),
        catchup=False,
        tags=['hw_11_a-vahterkina']
) as dag:

    t1 = PythonOperator(
        task_id='wtf',
        python_callable=func
    )

