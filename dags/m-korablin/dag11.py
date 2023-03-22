from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def sql_querry12():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id,count(*) FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count(*) DESC
                LIMIT 1
                """
            )
            return cursor.fetchone()

with DAG(
    'hw_11_m-korablin',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='sqlquery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['VanDarkholme'],
) as dag:

    task = PythonOperator(
        task_id='sql_query',
        python_callable=sql_querry12
    )


    task
    