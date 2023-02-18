from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


def get_db():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:  
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                    SELECT
                        user_id, 
                        COUNT(action)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(action) DESC
                """
            )
            return cursor.fetchone()


with DAG(
        'hw_11_a-tjurin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
        },

        description='Task 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 18),
        catchup=False,

        tags=['Task_11'],
) as dag:
    
    t1 = PythonOperator(
        task_id='get_db_req',
        python_callable=get_db
    )

    t1
