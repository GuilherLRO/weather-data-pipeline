from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

from custom_functions.weather_data import get_cities_to_weather, create_current_weather_file, create_historical_weather_file , create_forecast_weather_file
from custom_functions.queries import create_curated_current,update_curated_current,create_curated_timeline,update_curated_timeline

from datetime import datetime, timedelta
import pandas as pd
import os
import uuid

import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'weather_data',
    default_args=default_args,
    description='Pipeline DAG',
    schedule_interval='@once',
    catchup=False
)

# Define dag tasks

def print_hello():
    logging.info("Hello World")

def bye_bye():
    logging.info("Bye Bye")

def download_current_weather_data():
    cities = get_cities_to_weather()
    save_path = os.getcwd() + '/dags/'
    logging.info(f"Downloading current weather data for {cities}")
    create_current_weather_file(cities, save_path = save_path)    
    logging.info(save_path)
    logging.info(os.listdir(save_path))
    read_path = save_path + 'current_raw.csv'
    df = pd.read_csv(read_path)
    logging.info(df.head())

def download_forecast_weather_data():
    cities = get_cities_to_weather()
    save_path = os.getcwd() + '/dags/'
    logging.info(f"Downloading forecast weather data for {cities}")
    create_forecast_weather_file(cities, save_path = save_path)
    logging.info(save_path)
    logging.info(os.listdir(save_path))
    read_path = save_path + 'forecast_raw.csv'
    df = pd.read_csv(read_path)
    logging.info(df.head())

def download_historical_weather_data():
    cities = get_cities_to_weather()
    save_path = os.getcwd() + '/dags/'
    logging.info(f"Downloading historical weather data for {cities}")
    create_historical_weather_file(cities, save_path = save_path)
    logging.info(save_path)
    logging.info(os.listdir(save_path))
    read_path = save_path + 'history_raw.csv'
    df = pd.read_csv(read_path)
    logging.info(df.head())

def current_weather_to_raw():

    current = pd.read_csv('./dags/current_raw.csv')
    current['uuid'] = [uuid.uuid4() for _ in range(len(current))]
    
    #put uuid in the first column
    cols = current.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    current = current[cols]

    current['update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current.columns = [c.replace('localtime', 'local_time') for c in current.columns]
    logging.info(current.head())
    columns = current.columns
    try:
        conn = psycopg2.connect("dbname='postgres' user='root' host='172.19.0.1' password='root'")
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS current_weather_raw")
        cur.execute(f"CREATE TABLE IF NOT EXISTS current_weather_raw ({' text,'.join(columns)} text)")
        # add values to the table based on the csv file
        for index, row in current.iterrows():
            cur.execute(f"INSERT INTO current_weather_raw ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})", row)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table created successfully")
    except Exception as e:
        logging.info(f"Error {e}")
        raise e

def forecast_weather_to_raw():
    forecast = pd.read_csv('./dags/forecast_raw.csv')
    forecast['day'] = forecast['day'].str.replace("'", '"')
    forecast['uuid'] = [uuid.uuid4() for _ in range(len(forecast))]
    #put uuid in the first column
    cols = forecast.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    forecast = forecast[cols]

    forecast['update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    forecast.columns = [c.replace('localtime', 'local_time') for c in forecast.columns]
    logging.info(forecast.head())
    columns = forecast.columns
    try:
        conn = psycopg2.connect("dbname='postgres' user='root' host='172.19.0.1' password='root'")
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS forecast_weather_raw")
        cur.execute(f"CREATE TABLE IF NOT EXISTS forecast_weather_raw ({' text,'.join(columns)} text)")
        # add values to the table based on the csv file
        for index, row in forecast.iterrows():
            cur.execute(f"INSERT INTO forecast_weather_raw ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})", row)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table created successfully")
    except Exception as e:
        logging.info(f"Error {e}")
        raise e
    
def history_weather_to_raw():   
    history = pd.read_csv('./dags/history_raw.csv')
    history['day'] = history['day'].str.replace("'", '"')
    history['uuid'] = [uuid.uuid4() for _ in range(len(history))]
    #put uuid in the first column
    cols = history.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    history = history[cols]

    history['update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history.columns = [c.replace('localtime', 'local_time') for c in history.columns]
    logging.info(history.head())
    columns = history.columns
    try:
        conn = psycopg2.connect("dbname='postgres' user='root' host='172.19.0.1' password='root'")
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS history_weather_raw")
        cur.execute(f"CREATE TABLE IF NOT EXISTS history_weather_raw ({' text,'.join(columns)} text)")
        # add values to the table based on the csv file
        for index, row in history.iterrows():
            cur.execute(f"INSERT INTO history_weather_raw ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})", row)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table created successfully")
    except Exception as e:
        logging.info(f"Error {e}")
        raise e
    
def current_to_curated():
    try:
        conn = psycopg2.connect("dbname='postgres' user='root' host='172.19.0.1' password='root'")
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS current_weather")
        #create table
        cur.execute(create_curated_current)
        # add values to the table based on the raw
        cur.execute(update_curated_current)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table(current) updated successfully")
    except Exception as e:
        logging.info(f"Error {e}")
        raise e

def timeline_to_curated():
    try:
        conn = psycopg2.connect("dbname='postgres' user='root' host='172.19.0.1' password='root'")
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS timeline_weather")
        #create table
        cur.execute(create_curated_timeline)
        # add values to the table based on the raw
        cur.execute(update_curated_timeline)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table timeline updated successfully")
    except Exception as e:
        logging.info(f"Error {e}")
        raise e
     
# Add the operators to the DAG
hello = PythonOperator(
    task_id='hello',
    python_callable=print_hello,
    dag=dag
)

bye_bye = PythonOperator(
    task_id='bye_bye',
    python_callable=bye_bye,
    dag=dag
)

download_current_weather_data = PythonOperator(
    task_id='download_current_weather_data',
    python_callable=download_current_weather_data,
    dag=dag
)

download_forecast_weather_data = PythonOperator(
    task_id='download_forecast_weather_data',
    python_callable=download_forecast_weather_data,
    dag=dag
)

download_historical_weather_data = PythonOperator(
    task_id='download_historical_weather_data',
    python_callable=download_historical_weather_data,
    dag=dag
)

current_weather_to_raw = PythonOperator(
    task_id='current_weather_to_raw',
    python_callable=current_weather_to_raw,
    dag=dag
)

forecast_weather_to_raw = PythonOperator(
    task_id='forecast_weather_to_raw',
    python_callable=forecast_weather_to_raw,
    dag=dag
)

history_weather_to_raw = PythonOperator(
    task_id='history_weather_to_raw',
    python_callable=history_weather_to_raw,
    dag=dag
)

current_to_curated = PythonOperator(
    task_id='current_to_curated',
    python_callable=current_to_curated,
    dag=dag
)

timeline_to_curated = PythonOperator(
    task_id='timeline_to_curated',
    python_callable=timeline_to_curated,
    dag=dag
)

# Configure the task dependencies 

hello >> download_current_weather_data
hello >> download_forecast_weather_data
hello >> download_historical_weather_data

download_current_weather_data >> current_weather_to_raw >> current_to_curated >> bye_bye
download_forecast_weather_data >> forecast_weather_to_raw >> timeline_to_curated >> bye_bye
download_historical_weather_data >> history_weather_to_raw >> timeline_to_curated >> bye_bye