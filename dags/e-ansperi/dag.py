from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Function to retrieve the user who has the most likes
def get_top_user(**kwargs):
    query = '''
    SELECT user_id, COUNT(*) as count 
    FROM "feed_action" 
    WHERE action = 'like' 
    GROUP BY user_id 
    ORDER BY count DESC 
    LIMIT 1
    '''

    # Using PostgresHook to connect to the database and execute the query
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            top_user = {'user_id': result[0], 'count': result[1]}
            print(top_user)  # Additional print statement for debugging

    # The returned value will be saved to XCom
    return top_user


# Define the DAG, its schedule, and set it to run
dag = DAG(
    'hw_e-ansperi_11',
    default_args=default_args,
    description='A DAG with a PythonOperator to find the user with the most likes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False
)

# Task using PythonOperator to retrieve and return the top user
top_user_task = PythonOperator(
    task_id='get_top_user_task',
    python_callable=get_top_user,
    provide_context=True,
    dag=dag
)
