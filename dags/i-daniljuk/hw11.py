"""
PythonOperator and xcom
"""
from datetime import datetime, timedelta
# from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'hw_10_i-daniljuk',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    description='A cycle tasks DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['i-daniljuk'],
) as dag:
    
    def find_top_liker():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT user_id, COUNT(user_id) as count
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                LIMIT 1;
                """)
                result = cursor.fetchone()
                return result
    
    
    def get_user(ti):
        pull_func = ti.xcom_pull(
            key='return_value',
            task_ids='find_top_liker'
        )
        print(pull_func)


    t1 = PythonOperator(
        task_id='show_top_liker',
        python_callable=get_user,
    )
