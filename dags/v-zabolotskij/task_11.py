from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def get_connect():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    
    with postgres.get_conn() as conn:
         with conn.cursor() as cursor:
            cursor.execute ("""
            select user_id, count(*) as count_likes from feed_action
            where action = 'like'
            group by user_id
            order by count(*) desc
            limit 1           
            """)
         return print(cursor.fetchone())   
   
with DAG\
    (
    "task_11_v_zabolotskij",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "DAG for task #11",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 20),
    catchup = False,
    tags = ["task_11"]
    ) as dag:
        
        task_1 = PythonOperator(
            task_id = "find_top_like_user",
            python_callable = get_connect  
        )