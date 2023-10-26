from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'hw_o-jugaj_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
        description='hw_o-jugaj_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 23),
        catchup=False,
        tags=['hw_o-jugaj_11'],
    ) as dag:

        def get_connection():
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from psycopg2.extras import RealDictCursor

            postgres = PostgresHook(postgres_conn_id='startml_feed')
            with postgres.get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        f"""
                        SELECT user_id, COUNT(user_id)
                        FROM feed_action
                        WHERE action = 'like'
                        GROUP BY user_id
                        ORDER BY COUNT(user_id) DESC
                        LIMIT 1
                        """
                    )
                    result = cursor.fetchone()
                    return result



        t1 = PythonOperator(
            task_id='get_connection',
            python_callable=get_connection
        )