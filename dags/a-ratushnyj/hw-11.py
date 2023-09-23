"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook, RealDictCursor

def get_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = """
            select 
            fa.user_id,
            count(*) as cnt
            from feed_action fa
            where fa.action = 'like'
            group by 1
            order by count(*) desc
            limit 1
            """
            cursor.execute(query)
            result = cursor.fetchone()
            return result

with DAG(
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},

        start_date=datetime(2023, 9, 22),
        dag_id="hw_11_a-ratushnyj",
        schedule_interval=timedelta(days=1),
        tags=['hw-11'],

) as dag:

    get_data =  PythonOperator(
        task_id = 'get_user_with_likes',
        python_callable=get_user
    )
    get_data