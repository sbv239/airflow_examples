from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'ignatev_dag_07',
    default_args=default_args,
    start_date=datetime(2022, 4, 15),
    max_active_runs=1,
    schedule_interval=timedelta(days = 1),
) as dag:

    def conn_get():
        from airflow.providers.postgres.operators.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor

        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                    '''
                    SELECT user_id, COUNT(action)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(action) DESC
                    LIMIT 1
                    '''
                    )
                    return cursor.fetchone()
        

    t1 = PythonOperator(
        task_id='get_top_user',
        python_callable=conn_get,
    )