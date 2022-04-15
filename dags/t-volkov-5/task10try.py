from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


with DAG(
    'hw_test_t-volkov-5',
    default_args=default_args,
    description='God bless my creature',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    def get_most_active_user(**kwargs):
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory = RealDictCursor) as cursor:
                cursor.execute(
                    '''
                    SELECT user_id, count(*) FROM "feed_action" WHERE action = 'like' GROUP BY user_id ORDER BY count(*) DESC LIMIT 1
                    ''')
                query_result = cursor.fetchall()
                cursor.close()
                print(query_result)
                return query_result

    t1 = PythonOperator(
        task_id='push_var_into_xcom',
        python_callable=get_most_active_user
    )

    t1
