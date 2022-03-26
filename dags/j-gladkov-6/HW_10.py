from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta
from textwrap import dedent

def get_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                    """
                       SELECT user_id, COUNT(user_id)
                       FROM feed_action
                       WHERE action = 'like'
                       GROUP BY user_id
                       ORDER BY COUNT(user_id) DESC
                       LIMIT 1
                       """
                       )
            print(cursor.fetchone())

with DAG(
    'hw_10_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "Connections", # name
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 22),
    catchup = False,
    tags = ['hw_10'],
) as dag:

    t1 = PythonOperator(
        task_id = 'user_gave_the_most_likes',
        python_callable = get_connection,
    )

    t1
