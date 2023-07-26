from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime
from psycopg2.extras import RealDictCursor


from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG('hw_a-gajchuk-22_11',
         default_args={
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),
                    },
         description = 'Dag for hw11',
         start_date = datetime(2023, 7, 25),
         tags = ["agaychuk11"]) as dag:

    def getuser():

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("SELECT user_id, COUNT(*) AS count FROM feed_action WHERE action = 'like' GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 1")
                    res = cursor.fetchone()
        return res

    t2 = PythonOperator(task_id = 'get_user',
                        python_callable = getuser)
        


    t2
    
