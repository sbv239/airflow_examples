from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.Python import PythonOperator

with DAG(
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

) as dag:
    t1 = BashOperator(
        task_id="test_e",
        bash_command="pwd ",
    )

    def print_ds(ds, **kwargs):
        print(kwargs)
        return print(ds)

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )
    t1 >> t2