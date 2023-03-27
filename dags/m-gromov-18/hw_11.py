from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'hw_11_m-gromov-18',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for unit 11',
        tags=['DAG-11_m-gromov-18'],
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 26),

) as dag:
    def get_connect():
        from airflow.providers.postgres.operators.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, count(1)
                    FROM feed_action
                    WHERE action='like' 
                    GROUP BY 1
                    ORDER BY 2 desc
                    LIMIT 1
                    """
                )
                return cursor.fetchone()

    t1 = PythonOperator(
        task_id='connection',
        python_callable=get_connect
    )