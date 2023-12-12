from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
#from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from Ingestion import ETL

#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Ebube',
    'start_date': days_ago(0),
    #'end_date': datetime(),
    #'depends_on_past': False,
    'email': ['ezenwajiebube@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='Stocks Data',
    default_args=default_args,
    description='ETL DAG using Python to extract data from api',
    schedule_interval=timedelta(days=1)
    #schedule_interval='@once', # @hourly, @daily, @weekly, @monthly, @yearly	
)

# define the tasks
Run_ETL = PythonOperator(
    task_id= 'Stocks Data ETL',
    python_callable= ETL(),
    dag= dag
)

# task pipeline
Run_ETL

if __name__ == "__main__":
    dag_python.cli()