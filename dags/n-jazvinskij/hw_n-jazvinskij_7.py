from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'hw_11_ex_7-n-jazvinskij',
    default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    description = 'hw_11_ex_7',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 10, 21),
    catchup = False,
) as dag:

    def python_20_tasks(task_number, ts, run_id):
        return f"task number is: {task_number}", f"task ts is: {ts}", f"task run_id is: {run_id}"

    for i in range(20):
        t1 = PythonOperator(
            task_id='Python_task_'+str(i),
            python_callable= python_20_tasks,
            op_kwargs = {'task_number': i}
        )
    t1