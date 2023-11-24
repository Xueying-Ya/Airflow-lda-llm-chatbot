#import pyarrow.parquet as pq
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import string
import numpy as np
import pandas as pd
import random
import os
import shutil
import datetime as dt
import json
from services.model.pub_api import retrieve_new_articles
from services.model.vector_store import create_vector_store,load_vector
from services.model.lda_model import process_input_lda,lda_model_save_data
from dotenv import load_dotenv

load_dotenv('./config/.env')
scopus_api_key = os.environ.get('SCOPUS_API_KEY')
VECTOR_STORE_PATH  = "./dags/services/vector_store_folder"

ALl_RAW_DATA_PATH = './dags/services/data/new_data.csv'
CURRENT_YEAR_RAW_DATA_PATH = './dags/services/data/new_data_2023.csv'

LDA_ALL_DATA_YEAR_PATH = './dags/services/data/lda_all_data_2015_2023.csv'
LDA_CURRENT_YEAR_DATA_PATH = './dags/services/data/lda_data_2023.csv'

# Get current date
current_date = datetime.now()

# Format the date as '20-November-2022'
formatted_date = current_date.strftime('%d-%B-%Y')

query = f"SOCI OR DENT OR DECI OR PHYS OR CENG OR EART OR VETE OR PHAR OR ENGI OR ECON OR ARTS OR IMMU OR MULT OR MATH OR NEUR OR PSYC OR AGRI OR BUSI OR NURS OR CHEM OR COMP OR ENVI OR MATE OR MEDI OR BIOC OR ENER OR HEAL AND PUBDATETXT({formatted_date})"

with open('./dags/services/sjr_data.json') as f:
    sjr_dic = json.load(f)

default_args = {
    "owner" : "Ya",
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow',
     default_args=default_args,
     start_date=datetime(2023, 11, 20),
     schedule_interval='@daily')

def data_sample_etl():

    @task()
    def get_new_raw_data():
        new_data = retrieve_new_articles(query, sjr_dic, scopus_api_key, result_number=5)
        print(new_data)
        return new_data
    
    @task()
    def save_new_raw_data_to_all_file(data,file_path=ALl_RAW_DATA_PATH):
        df = pd.DataFrame(data)
        try:
            df.to_csv(file_path, mode='a', header=False)
            print("Append data frame to CSV file")
        except Exception as e:
            print(e)
        return df
    
    @task()
    def save_new_raw_data_to_current_year_file(data,file_path=CURRENT_YEAR_RAW_DATA_PATH):
        df = pd.DataFrame(data)
        try:
            df.to_csv(file_path=CURRENT_YEAR_RAW_DATA_PATH, mode='a', header=False)
            print("Append data frame to CSV file")
        except Exception as e:
            print(e)
        return df
    
    @task()
    def update_lda_data_to_all_file(LDA_ALL=LDA_ALL_DATA_YEAR_PATH ,RAW_ALL=ALl_RAW_DATA_PATH):
        #ALl DATA
        corpus,id2word,tokenize_title = process_input_lda(RAW_ALL) #process all raw data
        result = lda_model_save_data(LDA_ALL,corpus,id2word,tokenize_title) #lda process
        return result
    
    @task()
    def update_lda_data_to_current_year_file(LDA_CURRENT_YEAR=LDA_CURRENT_YEAR_DATA_PATH,RAW_CURRENT_YEAR=CURRENT_YEAR_RAW_DATA_PATH):
        #CURREN YEAR DATA
        corpus,id2word,tokenize_title = process_input_lda(RAW_CURRENT_YEAR)
        result = lda_model_save_data(LDA_CURRENT_YEAR,corpus,id2word,tokenize_title)
        return result
    
    @task()
    def update_vector_store(VECTOR_STORE_PATH):
        print(VECTOR_STORE_PATH)
        if not os.listdir(VECTOR_STORE_PATH):
            print("empty")
            create_vector_store(VECTOR_STORE_PATH)
        else:    
            create_vector_store(VECTOR_STORE_PATH)
    
    data = get_new_raw_data()
    save_new_raw_data_to_all_file(data) #add new data to all data csv [2015-present]
    save_new_raw_data_to_current_year_file(data) #add new data to cuurent year [2023]
    lda_all_data = update_lda_data_to_all_file()
    lda_current_data = update_lda_data_to_current_year_file()
    update_vector_store(VECTOR_STORE_PATH)


data_sample_dag = data_sample_etl()


if __name__ == "__main__":
    print(VECTOR_STORE_PATH)