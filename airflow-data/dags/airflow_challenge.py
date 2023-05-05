import pandas as pd
from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.models import Variable
from extract_and_count import extract_orders, calculate_count  # Importing extract_orders and calculate_count functions
from io import open as io_open

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


## Do not change the code below this line ---------------------!!#
def export_final_output():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with io_open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    # Extract orders
    task_extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
        dag=dag
    )

    # Calculate count
    task_calculate_count = PythonOperator(
        task_id='calculate_count',
        python_callable=calculate_count,
        provide_context=True,
        dag=dag
    )

    # Export final output
    task_export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_output,
        provide_context=True,
        dag=dag
    )

    # Define the order of task execution
    task_extract_orders >> task_calculate_count >> task_export_final_output