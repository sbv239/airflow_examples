from airflow import DAG

from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
        'hw_l-anoshkina_11',

        default_args={
        'depends_on_past': False,
                           'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'HomeWork task11',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2023, 5, 30),

        catchup = False,

        ) as dag:

        def get_user():
            postgres = PostgresHook(postgres_conn_id="startml_feed")
            with postgres.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""select user_id, count(*)
                                        from feed_action
                                        where action = 'like'
                                        group by user_id
                                        order by count(*) desc
                                        limit 1""")

                    result = cursor.fetchone()
                    return result
        t1 = PythonOperator(
            task_id='get_user',
            python_callable=get_user
        )
        t1


