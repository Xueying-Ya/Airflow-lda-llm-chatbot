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
from services.model.vector_store import create_vector_store,load_vector,process_lda_files_for_vector_store
from services.model.lda_model import LDA  #,LDA_current_year
from dotenv import load_dotenv

load_dotenv('./config/.env')
scopus_api_key = os.environ.get('SCOPUS_API_KEY')

DATA_DIRECTORY = "./dags/services/data/"
VECTOR_STORE_PATH  = "./dags/services/vector_store_folder"

ALl_RAW_DATA_PATH = './dags/services/data/all_raw_data_2015-2023.csv'
CURRENT_YEAR_RAW_DATA_PATH = './dags/services/data/raw_data_2023.csv'

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
    "retry_delay" : timedelta(minutes=2)
}

@dag(dag_id='dag_with_taskflow',
     default_args=default_args,
     start_date=datetime(2023, 11, 20),
     schedule_interval='@daily', 
     concurrency=16
     )

def data_sample_etl():

    @task()
    def get_new_raw_data():
        """example data format :
          new_data =[{'publication_year': '2023',
        'journal_name': 'Biomedicine and Pharmacotherapy',
        'title': 'The recent advances in cell delivery approaches, biochemical and engineering procedures of cell therapy applied to coronary heart disease',
        'cited_count': '0',
        'country': 'China',
        'university': 'The Second Hospital of Jilin University',
        'author': 'Ma J.',
        'tier': 'Q1'},
        {'publication_year': '2023',
        'journal_name': 'Thin Solid Films',
        'title': 'In-situ/operando characterization of FeO<inf>x</inf>-based chemiresistive sensor of acetone vapours by X-ray absorption spectroscopy',
        'cited_count': '0',
        'country': 'Slovakia',
        'university': 'Institute of Physics Slovak Academy of Sciences',
        'author': 'Ivančo J.',
        'tier': 'Q2'},
        {'publication_year': '2023',
        'journal_name': 'Biomedicine and Pharmacotherapy',
        'title': 'Present and future of metabolic and metabolomics studies focused on classical psychedelics in humans',
        'cited_count': '0',
        'country': 'Spain',
        'university': 'Hospital del Mar',
        'author': 'Madrid-Gambin F.',
        'tier': 'Q1'}]"""
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
        return file_path
    
    @task()
    def save_new_raw_data_to_current_year_file(data,file_path=CURRENT_YEAR_RAW_DATA_PATH):
        df = pd.DataFrame(data)
        try:
            df.to_csv(file_path, mode='a', header=False)
            print("Append data frame to CSV file")
        except Exception as e:
            print(e)
        return file_path
    
    @task
    def current_year_LDA_analysis(raw_data_file_path,lda_file_path):
        word_group = LDA(raw_data_file_path,lda_file_path)
        return word_group

    @task
    def LDA_analysis(raw_data_file_path,lda_file_path,lda_data_directory):
        word_group = LDA(raw_data_file_path,lda_file_path)
        texts = process_lda_files_for_vector_store(lda_data_directory,file_seperator="***********************")
        return texts
    
    @task()
    def update_vector_store_locally_and_aws(lda_all_data,VECTOR_STORE_PATH):
        print(VECTOR_STORE_PATH)
        if not os.listdir(VECTOR_STORE_PATH):
            print("empty")
            create_vector_store(lda_all_data,VECTOR_STORE_PATH)
        else:    
            create_vector_store(lda_all_data,VECTOR_STORE_PATH)
    
    data = get_new_raw_data()
    raw_data_file_path_all = save_new_raw_data_to_all_file(data) #add new data to all data csv [2015-present]
    current_year_raw_data_file_path= save_new_raw_data_to_current_year_file(data) #add new data to cuurent year [2023]
    lda_current_data = current_year_LDA_analysis(current_year_raw_data_file_path,LDA_CURRENT_YEAR_DATA_PATH) # get lda result only 2023
    lda_all_data = LDA_analysis(raw_data_file_path_all,LDA_ALL_DATA_YEAR_PATH,DATA_DIRECTORY) # get lda result 2015- updated 2023
    update_vector_store_locally_and_aws(lda_all_data,VECTOR_STORE_PATH)

data_sample_dag = data_sample_etl()

if __name__ == "__main__":
    print(VECTOR_STORE_PATH)