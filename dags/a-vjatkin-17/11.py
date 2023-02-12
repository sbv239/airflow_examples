from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator




def get_user():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""                   
              select user_id, count(action)
              from feed_action
              where 1=1
                and action = 'like'
              group by user_id
              order by count(action) desc
              limit 1
              """)

            return cursor.fetchone()


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    'a-vjatkin-17_task_11',
    default_args=default_args,
    description='test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 12),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id=f"postgres_example",
        python_callable=get_user
    )
