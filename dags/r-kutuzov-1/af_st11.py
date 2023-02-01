from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor


def get_most_liker():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                    SELECT user_id AS user_id, Count(action) AS count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY 2 DESC
                    LIMIT 1
                """)
            return cursor.fetchone()


with DAG(
    'r-kutuzov-1_dag_11-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Airflow lesson step 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['r-kutuzov-1_step_11'],
) as dag:
    
    task = PythonOperator(
        task_id=f'get_most_liker_with_Python', 
        python_callable=get_most_liker,
    )

    task
