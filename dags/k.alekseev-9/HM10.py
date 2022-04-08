from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'hw_10_v-k.alekseev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Tenth DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['T10'],
) as dag:


    def user_max_likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT user_id, COUNT(user_id)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(user_id) DESC
                    LIMIT 1 
                    '''
                )
                result = cursor.fetchone()
                return result


    t1 = PythonOperator(
        task_id='user_max_likes',
        python_callable=user_max_likes,
    )