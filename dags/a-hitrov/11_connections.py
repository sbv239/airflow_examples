from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator



def most_liking_user():
    # Прямолинейный вариант

    #import psycopg2
    #from airflow.hooks.base import BaseHook
    #from psycopg2.extras import RealDictCursor
    #creds = BaseHook.get_connection(id соединения)
    #db_url = (f"postgresql://{creds.login}:{creds.password}"
    #          f"@{creds.host}:{creds.port}/{creds.schema}")
    #with psycopg2.connect(db_url, cursor_factory=RealDictCursor) as conn:
    #    with conn.cursor() as cursor:
    #        # ...

    # Альтернативный вариант

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import RealDictCursor

    pg = PostgresHook(postgres_conn_id='startml_feed')
    with pg.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('''

                SELECT user_id, COUNT(user_id)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(user_id)
                LIMIT 1

            ''')
            user = cursor.fetchone()
    return user


dag_params = {
    'dag_id': 'xxa11-connection',
    'description': 'Использование базы данных',
    'start_date': datetime(2023, 6, 7),
    'schedule_interval': timedelta(days=365),
    'default_args': {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    'catchup': True,
    'tags': ['xxa'],
}

with DAG(**dag_params) as dag:

    PythonOperator(
        task_id='most_liking_user',
        python_callable=most_liking_user,
    )


if __name__ == '__main__':
    # AirFlow 2.6.2
    # https://airflow.apache.org/docs/apache-airflow/2.6.2/core-concepts/executor/debug.html
    #dag.test()

    # AirFlow 2.2.4
    # https://airflow.apache.org/docs/apache-airflow/2.2.4/executor/debug.html
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
