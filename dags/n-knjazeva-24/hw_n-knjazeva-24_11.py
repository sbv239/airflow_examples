from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.hooks.base import BaseHook



with DAG(
        'hw_n-knjazeva-24_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 9, 21),
        schedule_interval=timedelta(days=1)
) as dag:
    def get_user():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT f.user_id, COUNT(f.user_id)
                    FROM feed_action f
                        WHERE f.action = 'like'
                        GROUP BY f.user_id
                        ORDER BY COUNT(f.post_id) DESC
                        LIMIT 1
                    """
                )
                result = cursor.fetchone()
                return result


    t1 = PythonOperator(
        task_id='get_conn',
        python_callable=get_user,
    )