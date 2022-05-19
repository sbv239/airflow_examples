from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from psycopg2.extras import RealDictCursor
import psycopg2

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def get_user_base():
    creds = BaseHook.get_connection(postgres_conn_id="startml_feed")
    sql_str = """
        select 
        user_id,
        count(*)
        from feed_action 
        where 
        action = 'like'
        group by 
        user_id
        order by 
        count(*) desc 
        limit 1
        ;
        """
    conn_id = (
        f"postgresql://{creds.login}:{creds.password}"
        f"@{creds.host}:{creds.port}/{creds.schema}"
    )
    with psycopg2.connect(conn_id) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql_str)
            result = cursor.fetchall()

    return result

def get_user():
    #creds = BaseHook.get_connection(postgres_conn_id="startml_feed")
    sql_str = """
        select 
        user_id,
        count(*)
        from feed_action 
        where 
        action = 'like'
        group by 
        user_id
        order by 
        count(*) desc 
        limit 1
        ;
        """
    #conn_id = (
    #    f"postgresql://{creds.login}:{creds.password}"
    #    f"@{creds.host}:{creds.port}/{creds.schema}"
    #)
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql_str)
            result = cursor.fetchall()

    return result



with DAG(
    'lesson11_s-kosouhov_task_10',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 10),
    catchup=False,
    tags=['task_10'],
) as dag:

    task_1 = PythonOperator(
        task_id = f"task_get_user",
        python_callable=get_user,
    )

    task_1
    