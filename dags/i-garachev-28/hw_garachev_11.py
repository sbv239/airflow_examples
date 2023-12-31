from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_garachev_11_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_11'],
) as dag:

    def get_user():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn("postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml") as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT user_id, COUNT(user_id) as count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count DESC
                    LIMIT 1''')
                return cursor.fetchone()

    t1 = PythonOperator(
        task_id='hw_11_garachev',
        python_callable=get_user
    )

    t1