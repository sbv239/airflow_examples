from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
 
with DAG(
        'firstdag',
        default_args = {
            'depends_on_past':False,
            'email':['pobol.email@yandex.ru'],
            'email_on_failure':True,
            'email_on_retries':False,
            },
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022,12,22)
        ) as dag:
    
    ### FUNC
    def print_s(task_number):
        print(task_number)

    for i in range(30):
        if i < 10:
            task = BashOperator(
                    task_id = 'bash_task_' + str(i),
                    bash_command = f'echo {i}'
                    )
            
            task.doc_md = dedent(
                    """
                    `code`
                    __полужирный__
                    _курсив_
                    # Документация
                  """
                    )


        else:
            task = PythonOperator(
                    task_id = 'python_task_' + str(i),
                    python_callable = print_s,
                    op_kwargs = {'task_number': {f"task number is: {i}"}})

            task.doc_md = dedent(
                    """
                    `code`
                    __полужирный__
                    _курсив_
                    # Документация
                    """
                    )
        task.doc_md = dedent(
                    """
                    `code`
                    __полужирный__
                    _курсив_
                # Документация
                    """
                    )
        





