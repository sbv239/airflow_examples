from airflow.operators.bash import BashOperator

with DAG('task2', description='Solution of task2',) as dag:
    t1 = BashOperator(
        task_id='print pwd',
        bash_command='pwd',
    )
    def print_context(ds, **kwargs):
    
        print(kwargs)
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='print_the_context',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2

    