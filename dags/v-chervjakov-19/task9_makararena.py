"""
hw_10.py DAG
"""
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
#import psycopg2
from airflow.hooks.base import BaseHook
#from psycopg2.extras import RealDictCursor
from airflow.models import Variable

# def connect_to_database():
#     creds = BaseHook.get_connection("startml_feed")
#     with psycopg2.connect(
#         f"postgresql://{creds.login}:{creds.password}"
#         f"@{creds.host}:{creds.port}/{creds.schema}"
#     ) as conn:
#         with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#             cursor.execute("""
#             SELECT user_id, count(action) as count
#             FROM feed_action
#             WHERE action = 'like'
#             GROUP BY user_id
#             ORDER BY 2 DESC
#             LIMIT 1
#             """)
#             result = cursor.fetchone()
#     return result
def get_variable():
    is_startml = Variable.get('is_startml')
    print(is_startml)

with DAG(
        'makararena',  # уникальное имя DAG
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='Lesson 11, Task 10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,

        tags=['Lesson_11_Task_11'],
) as dag:
    #is_prod = Variable.get('is_startml')
    # отправляем значения
    t1 = PythonOperator(
        task_id='push_data_out',
        python_callable=get_variable
    )
t1 