from datetime import timedelta, datetime


from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_arg():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                select user_id, count(post_id) as count_like
                from feed_action
                where action = 'like'
                group by user_id
                order by count_like desc
                limit 1
                """
            )
            result = cursor.fetchone()
    print(result)
    return {"user_id" : result[0], 'count' : result[1]}



with DAG(
    'hw_d-oreshnikov_11',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime(2023, 1, 1)
) as dag:
    
    t1 = PythonOperator(
            task_id = f'get_arg',
            python_callable= get_arg
        )
    
t1 