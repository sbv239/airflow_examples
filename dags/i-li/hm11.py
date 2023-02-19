import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def get_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
      with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute('''
        SELECT user_id, count(action)
        FROM "feed_action"
        WHERE action = 'like'
        GROUP BY user_id
        ORDER BY count(action) DESC
        ''')
        result=cursor.fetchall()
        return result[0]

with DAG(
        'hm_11_i-li',
        default_args={
            'dependes_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_in_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2023, 2, 15)
) as dag:
    t1 = PythonOperator(
        task_id='hm_11-i-li',
        python_callable=get_connection
    )
