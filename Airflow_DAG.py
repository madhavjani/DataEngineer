# from airflow import DAG
# from airflow.operators.bash import BashOperator
# import pendulum
# from datetime import datetime,timedelta
# import  os
# from airflow.operators.python import PythonOperator
#
# from Extract import process_symbol_date
#
# local_tz = pendulum.timezones("US/Eastern")
#
# args = {
#     'owner' : 'Madhav',
#     'start_date' : pendulum.datetime(2023,5,25,tzinfo=local_tz),
#     'email' : ['madhavjani1996@gmail.com'],
#     'email_on_failure' : 'True',
#     'email_on_retry' : 'True',
#     'retries' : '3',
#     'retry_delay' : timedelta(minutes=5)
# }
#
# with DAG('symbol_dag',
#          default_args=args,
#          schedule_interval='@daily') as dag:
#
#     process_raw_data_task = PythonOperator(
#         task_id='process_raw_data',
#         python_callable=process_symbol_date,
#         op_kwargs={
#             'raw_data_path': 'stock_data/raw_data/',
#             'processed_data_path': 'stock_data/processed_data/'
#         }
#     )

import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='Test_Dag',
    schedule_interval='* * * * *',
    start_date=datetime(year=2023,day=1,month=5),
    catchup = False
) as dag:

    task_get_datetime=BashOperator(task_id='get_time',bash_command='date')
