from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


with DAG(
    'intro_10th',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='intro_10th',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 10),
    catchup=False,
    tags=['a-jablokova'],
) as dag:

    def get_user_w_most_likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute('''
                    SELECT f.user_id, COUNT(*)
                    FROM "feed_action" f
                    WHERE f.action = 'like'
                    GROUP BY f.user_id
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                ''')
                result = cursor.fetchone()
        print(result)
        return result

    t1 = PythonOperator(
        task_id='get_user_w_most_likes',
        python_callable=get_user_w_most_likes,
        )

    t1