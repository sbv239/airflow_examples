from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'hw_11_l-ivanenkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_11_l-ivanenkov',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_11_l-ivanenkov'],
) as dag:
    postgres = PostgresHook(postgres_conn_id="startml_feed")


    def most_like():
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                              SELECT user_id, COUNT(user_id)
                              FROM feed_action 
                              WHERE action = 'like'
                              GROUP BY user_id
                              ORDER BY COUNT(user_id) DESC
                              LIMIT 1            
                          """)
                return cursor.fetchall()


    t1 = PythonOperator(
        task_id='find_most_like',
        python_callable=most_like
    )
