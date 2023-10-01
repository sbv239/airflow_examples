from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'connection',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "connection"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_11'],
) as dag:

    def get_connection():
        from airflow.hooks.base import BaseHook
        import psycopg2
        from psycopg2.extras import RealDictCursor

        creds = BaseHook.get_connection('startml_feed')
        with psycopg2.connect(
            f'postgresql://{creds.login}:{creds.password}'
            f'@{creds.host}:{creds.port}/{creds.schema}',
            cursor_factory=RealDictCursor
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT user_id, COUNT(user_id) AS count FROM "feed_action"
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count DESC
                    """
                )
                res = dict(cursor.fetchone())
                cursor.close()
        return res

    PythonOperator(
        task_id='get_connection',
        python_callable=get_connection,
    )

    if __name__ == '__main__':
        dag.test()