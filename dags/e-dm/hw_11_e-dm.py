"""\
####hw_11_e-dm_dag 
#something
`code`
_code_
**bold**
*ital*
"""  
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresHook

def db_operation():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id,count(*) from feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count(*) desc
                LIMIT 1
                """
            )
            return cursor.fetchone()
with DAG(
    'hw_11_e-dm_dag',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description='lesson_11_task_11',
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['hw_11_e-dm_tag'],
) as dag:

    t1 = PythonOperator(
        task_id = 'db_operation',
        python_callable = db_operation
    )
	# Последовательность задач:
    t1