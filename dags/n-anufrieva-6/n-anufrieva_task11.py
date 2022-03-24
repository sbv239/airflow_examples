from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def get_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                   SELECT
                        user_id,
                        COUNT(user_id) AS likes
                   FROM feed_action
                   WHERE action = 'like'
                   GROUP BY 1
                   ORDER BY 2 DESC
                   LIMIT 1
                   """
            )
            print(cursor.fetchone())


with DAG(
        'n-anufrieva_task11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='n-anufrieva_task11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['n-anufrieva_task11'],
) as dag:

    task_1 = PythonOperator(
        task_id='get_user_likes',
        python_callable=get_connection,
    )

    task_1
