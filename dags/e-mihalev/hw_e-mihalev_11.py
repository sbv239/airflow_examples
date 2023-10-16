from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
'hw_e-mihalev_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple DAG',
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['example']
) as dag:
    def task_pg():
        postgres = PostgresHook(postgres_conn_id= "startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT user_id, COUNT(action) as count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count desc
                    LIMIT 1
                    """
                )
                return cursor.fetchone()
    t1 = PythonOperator(task_id='pg', python_callable=task_pg)
    t1