from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json




str_text = 'xcom test'


def task_10_func():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action) as total_like
                FROM feed_action
                WHERE feed_action.action = 'like'
                GROUP BY user_id
                ORDER BY total_like DESC
                LIMIT 1;
                """
            )
            result = cursor.fetchone()
            user_id = result['user_id']
            count = result['total_like']
            result_dict = {"user_id": user_id, "count": count}
        return result_dict



# Default settings applied to all tasks
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'hw_10_d-nikolaev',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_10_func,
    )

