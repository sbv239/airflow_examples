from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook



default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    'hw_11_utrobina',
    catchup=False,
    default_args=default_args,
    schedule_interval='30 15 * * *',
    start_date=datetime(2023, 1, 21),
    description='etl',
    tags=['j-utrobina']
)

def pg_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""SELECT user_id, COUNT(*) AS count
                          FROM feed_action
                          WHERE action = 'like'
                          GROUP BY user_id
                          ORDER BY count DESC
                          LIMIT 1""")
            return cursor.fetchone()

task1 = PythonOperator(
        task_id='ps_user',
        dag=dag,
        python_callable=pg_user
    )

    
task1