from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import datetime,timedelta
import  os
from airflow.operators.python import PythonOperator

from Extract import process_symbol_date

local_tz = pendulum.timezones("US/Eastern")

args = {
    'owner' : 'Madhav',
    'start_date' : pendulum.datetime(2023,5,25,tzinfo=local_tz),
    'email' : ['madhavjani1996@gmail.com'],
    'email_on_failure' : 'True',
    'email_on_retry' : 'True',
    'retries' : '3',
    'retry_delay' : timedelta(minutes=5)
}

with DAG('stock_market_data_processing',
         default_args=args,
         schedule_interval='@daily') as dag:

    process_raw_data_task = PythonOperator(
        task_id='process_raw_data',
        python_callable=process_symbol_date,
        op_kwargs={
            'raw_data_path': 'stock_data/raw_data/',
            'processed_data_path': 'stock_data/processed_data/'
        }
    )
