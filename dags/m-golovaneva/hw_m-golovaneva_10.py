from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# def printing(**kwargs):
#     value = kwargs["statement"]
#     return value

def printing():
    value = "Airflow tracks everything"
    return value

def pulls_statement(ti):
    pulled_statement = ti.xcom_pull(
        key="return_value",
        task_ids="pull_statement"
    )
    print(pulled_statement)

with DAG(
        "hw_m-golovaneva_task10",

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='trying to play around XCom',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,

        tags=['mariaSG']

) as dag:

    # print_statement = PythonOperator(
    #     task_id="print_statement",
    #     python_callable=printing,
    #     op_kwargs={"statement":"Airflow tracks everything"}
    # )

    print_statement = PythonOperator(
        task_id="print_statement",
        python_callable=printing
    )

    pull_statement = PythonOperator(
        task_id="pull_statement",
        python_callable=pulls_statement
    )

    print_statement >> pull_statement















