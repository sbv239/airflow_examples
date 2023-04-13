from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import timedelta,datetime


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def get_id():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            select user_id, Count(user_id)
            from feed_action
            where action = 'like' 
            group by user_id
            order by COUNT(user_id) desc
            limit 1""")
            result = cursor.fetcone()
    return result

with DAG(
'il_shestov_task_9',
default_args = default_args,
schedule_interval= timedelta(days=1),
start_date = datetime(2023,4,12),
catchup = False
) as dag:
    t1 = PythonOperator(
        task_id = 'get_user',
        python_callable = get_id)
