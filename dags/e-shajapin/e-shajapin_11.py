from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'hw_11_e-shajapin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
) as dag:

    def connect():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                cursor.execute("""
                                SELECT user_id, COUNT(action)
                                FROM feed_action f
                                JOIN "user" u on f.user_id = u.id
                                WHERE action = 'like'
                                GROUP BY user_id
                                ORDER BY count DESC
                                """)
                result = cursor.fetchone()
                return result


    t = PythonOperator(task_id="connect", python_callable=connect)
