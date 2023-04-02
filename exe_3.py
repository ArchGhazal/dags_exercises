import time
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

from airflow.operators.dummy import DummyOperator 
import pandas as pd 
import numpy as np 
import os 


# define a function that acsept kwargs
def get_data(**kwargs):
    ticker=kwargs['ticker']
    api_key = "7OVDTRR4DLV5EGDH"
    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=' + ticker + &apikey='+ api_key'
    r = requests.get(url)

    try: 
        data = r.json()
        path = "/Users/MIQDAD/data_center/data_lake"
        with open(path+"stock_market_rowdata" + ticker +'_' +str(time.time()),"W" ) as outfile :
            json.dump(data,outfile)
    except:
        pass


    # here we define our task
    task_0 = PythonOperator(
        task_id="get_market_data",
        python_callable=get_data , op_kwargs = {'ticker' : "IBM"})

 
    
    


