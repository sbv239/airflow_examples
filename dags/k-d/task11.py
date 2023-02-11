"""
Напишите DAG, состоящий из одного PythonOperator. 
Этот оператор должен, используя подключение с conn_id="startml_feed", найти пользователя, 
который поставил больше всего лайков, и вернуть словарь {'user_id': <идентификатор>, 'count': <количество лайков>}. 
Эти значения, кстати, сохранятся в XCom.
"""


from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import timedelta, datetime

import psycopg2
from psycopg2.extras import RealDictCursor 

          
def get_sql_query():
	conn = psycopg2.connect(
		"postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"
        , cursor_factory=RealDictCursor
	)
	cursor = conn.cursor()
	cursor.execute(
		"""
        SELECT user_id, count(*) as likes 
        FROM "feed_action" 
        WHERE action = 'like' 
        GROUP BY user_ID
        ORDER BY likes DESC
        limit 1
		"""
	)
	return cursor.fetchone() 


with DAG (
    'k-d-t11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'description text',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023,1,1),
    catchup=False
) as dag:
    

    get_sql_query = PythonOperator(
        task_id = 'get_sql_query',
        python_callable=get_sql_query,
    )


