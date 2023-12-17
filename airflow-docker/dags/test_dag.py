from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# Define the DAG for test purposes thar only logs a message in the console and exits
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Test DAG',
    schedule_interval='@once',
    catchup=False
)

def print_hello():
    logging.info("Hello World")

def get_data():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame.from_dict(data['Time Series (Daily)']).T
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'date'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df['open'] = pd.to_numeric(df['1. open'])
    df['high'] = pd.to_numeric(df['2. high'])
    df['low'] = pd.to_numeric(df['3. low'])
    df['close'] = pd.to_numeric(df['4. close'])
    df['volume'] = pd.to_numeric(df['5. volume'])
    df.drop(columns=['1. open', '2. high', '3. low', '4. close', '5. volume'], inplace=True)
    df.to_csv('./dags/data.csv', index=False)

# add curent timestamp to table
def add_timestamp():
    try:
        conn = psycopg2.connect("dbname='postgres' user='root' host='172.19.0.1' password='root'")
        cur = conn.cursor()
        cur.execute("INSERT INTO test (criacao) VALUES (current_timestamp)")
        conn.commit()
        conn.close()
        logging.info("Timestamp added to table")
    except:
        logging.info("I am unable to connect to the database")

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
get_data_operator = PythonOperator(task_id='get_data_task', python_callable=get_data, dag=dag)
add_timestamp = PythonOperator(task_id='add_timestamp', python_callable=add_timestamp, dag=dag)


hello_operator >> get_data_operator
hello_operator >> add_timestamp