from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow import DAG

from psycopg2.extras import RealDictCursor
import psycopg2

import json

def get_user_with_likes():
    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
        f"postgresql://{creds.login}:{creds.password}"
        f"@{creds.host}:{creds.port}/{creds.schema}"
        ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute('''
                    SELECT user_id, count(*) AS count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count(*) DESC
                    LIMIT 1
                ''')
                result = cursor.fetchone()
            return result
        
with DAG(

    'hw_11_r-muratov-9',

    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    schedule_interval=timedelta(days=1),

    start_date=datetime(2023,5,20),

    catchup=False
    ) as dag:
                
        t1 = PythonOperator(
                task_id='get_biggest_fan',
                python_callable=get_user_with_likes,
        )

        t1
