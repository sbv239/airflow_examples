from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_10_a-samofalov',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    def get_db():
        return psycopg2.connect(
            'postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml',
            cursor_factory=RealDictCursor)


    def data_connection():
        with get_db().cursor() as cursor:
            cursor.execute(
                """SELECT user_id, COUNT(user_id) as count from feed_action
                where action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                limit 1;
                """)
            return cursor.fetchone()


    dag_db_samofalov = PythonOperator(
        task_id='db_connection_samofalov_hw_10',
        python_callable=data_connection
    )

    dag_db_samofalov
