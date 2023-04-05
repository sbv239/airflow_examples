from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator


def db_query():
    from airflow.hooks.base import BaseHook
    import psycopg2
    from psycopg2.extras import RealDictCursor

    creds = BaseHook.get_connection(conn_id="startml_feed")
    with psycopg2.connect(
    f"postgresql://{creds.login}:{creds.password}"
    f"@{creds.host}:{creds.port}/{creds.schema}",
    cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    COUNT(action)
                FROM
                    feed_action
                WHERE
                    action = 'like'
                GROUP BY
                    user_id
                ORDER BY
                    COUNT(action) desc
                LIMIT
                    1
                """
            )
            result = cursor.fetchone()
            return result

with DAG(
    'a-igumnov_task_11',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'hw_11_a-igumnov',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_11_a-igumnov_XCom']


) as dag:

    db_task = PythonOperator(
        task_id='find_op',
        python_callable=db_query
    )
    
    db_task            