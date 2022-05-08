from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from airflow.providers.postgres.operators.postgres import PostgresHook

def data_func():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect("startml_feed")
      with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT f.user_id, COUNT(f.user_id)
            FROM feed_action f 
            WHERE f.action = 'like'
            GROUP BY f.user_id
            ORDER BY COUNT(f.user_id) DESC
            LIMIT 1
            """
        )
        answer = cursor.fetchone()
    return answer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'xcom_connect_dag',
    start_date=datetime(2022, 5, 9),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'get_connection_DB',
        python_callable = data_func
    )
