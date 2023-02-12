"""
Step 11
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'petrashov_step_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        },
        description='step_11 - solution',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['step_11'],
) as dag:
    def get_user_by_max_likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                cursor.execute("""                   
                    SELECT
                      user_id,
                      count(*) as count
                    FROM
                      feed_action
                    WHERE
                      action = 'like'
                    GROUP BY
                      user_id
                    ORDER BY
                      count(*) desc
                    LIMIT 1
                """)
        return cursor.fetchone()


    t1 = PythonOperator(
        task_id='get_user',
        python_callable=get_user_by_max_likes
    )

    t1
