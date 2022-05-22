from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from psycopg2.extras import RealDictCursor
'''Напишите DAG, состоящий из одного PythonOperator. Этот оператор должен, используя подключение с 
conn_id="startml_feed", найти пользователя, который поставил больше всего лайков, и вернуть словарь 
{'user_id': <идентификатор>, 'count': <количество лайков>}. Эти значения, кстати, сохранятся в XCom.

'''
from airflow.providers.postgres.operators.postgres import PostgresHook

def postgres():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(''' 
                SELECT user_id, count(action) as count
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                LIMIT 1
                ''')
            res = cursor.fetchone()
            print(res)
            return res

with DAG(
    'hw_10_n-dmitrieva_postgress',
    default_args = { # Default settings applied to all tasks
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id = 'postgres', #task ID
        python_callable = postgres,
        )
    
   