from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

with DAG(
    'k-nevedrov-7-task_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:

    def find_user(**kwargs):
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    select u.id as user_id, count(a.action) as count
                    from feed_action as a
                    join "user" as u on a.user_id = u.id
                    where a.action = 'like'
                    group by u.id
                    order by count(a.action) desc
                    limit 1
                    """
                )

                user = cursor.fetchone()
                
                return user

    t1 = PythonOperator(
        task_id='find_user',  
        python_callable=find_user,  
    )