from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresHook


def get_most_active_user():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""
                select
                    user_id 
                    ,count(fa.action) "count"
                from feed_action fa 
                where fa.action  = 'like'
                group by user_id
                order by count(fa.action) desc
                limit 1
            """)
            result = cursor.fetchone()
    return result


def xcom_pull(ti):
    testing_increases = ti.xcom_pull(
        key='return_value',
        task_ids='get_most_active_user'
    )
    #print(testing_increases)

with DAG(
        'e-kim-18_task_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A DAG for task 02',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 20),
        catchup=False,
        tags=['e-kim-18-tag'],
) as dag:
    t1 = PythonOperator(
        task_id = 'get_most_active_user',
        python_callable=get_most_active_user,
    )

    t1