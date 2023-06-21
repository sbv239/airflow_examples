from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

def find_top_user():
    postgres = PostgresHook(postgres_conn_id='startml_feed')

    with postgres.get_conn() as conn: 
        with conn.cursor() as cursor:
            cursor.execute(
              '''
              select user_id, count(user_id)
              from feed_action
              where action = 'like'
              group by user_id
              order by count(user_id) desc
              limit 1
              ''')

            result = cursor.fetchall()
    return result

with DAG(
    'hw_a-chernova-21_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='A new dag for db',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 20),
    catchup=False,
    tags=['xcom_dt']
) as dag:
    t1 = PythonOperator(
        task_id = 'find_user',
        python_callable=find_top_user)
        
        