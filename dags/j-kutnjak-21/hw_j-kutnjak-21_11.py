from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor



default_args = {'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                }

with DAG(
    'hw_j-kutnjak-21_11',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 30),
    catchup=False,
    tags=['example'],
) as dag:

    def get_data():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    select 
                            user_id,
                            count(time) as "count"
                    from feed_action
                            where action = 'like'
                    group by user_id
                    order by count(time) desc
                    limit 1
                    """
                )
                return cursor.fetchone()


    run_python = PythonOperator(
        task_id='hw_j-kutnjak-21_11',
        python_callable=get_data
    )
