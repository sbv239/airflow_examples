from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_10',
         default_args=default_args,
         description='hw_d-shestak_10',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_10_d-shestak']
         ) as dag:
    def find():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, COUNT(action)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(action) DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchone()
            
    t1 = PythonOperator(
        task_id='sql_feed',
        python_callable=find
    )