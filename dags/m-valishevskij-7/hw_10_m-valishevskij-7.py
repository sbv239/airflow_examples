from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


with DAG(
        'hw_10_m-valishevskij-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 14),
        catchup=False,
        tags=['valishevskij']
) as dag:
    def get_user_id():
        from airflow.providers.postgres.operators.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor

        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, count(action) as count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP by 1
                    ORDER BY 2 DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchone()

    t1 = PythonOperator(
        task_id='hw_10_m-valishevskij-7_1',
        python_callable=get_user_id
    )


