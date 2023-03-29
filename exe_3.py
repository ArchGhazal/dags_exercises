import time
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def get_data():
    api_key = "7OVDTRR4DLV5EGDH"
    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey='+ api_key
    r = requests.get(url)

    try: 
        data = r.json()
        path = "/Users/MIQDAD/data_center/data_lake"
        with open(path+"stock_market_rowdata"+'IBM'+str(time.time()),"W" ) as outfile :
            json.dump(data,outfile)
    except:
        pass
# create the DAG which calls the python logic 
default_dag_args = {
    'start_date': datetime(2022, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1}

with DAG(
    "market_data_alphavantage_dag",
    schedule_interval='@daily',
    catchup=False,
    default_args=default_dag_args) as dag:

    # here we define our task
    task_0 = PythonOperator(
        task_id="get_market_data",
        python_callable=get_data)
    


