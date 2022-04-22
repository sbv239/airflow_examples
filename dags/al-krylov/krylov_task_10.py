from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


with DAG(
    'krylov_task_10',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 20),
    catchup=False
) as dag:

    def get_user():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT user_id, count(user_id)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(user_id) DESC
                    LIMIT 1
                    """,
                )
                results  = cursor.fetchone()
                return results

    task = PythonOperator(
        task_id='get_user',
        python_callable=get_user,
    )







