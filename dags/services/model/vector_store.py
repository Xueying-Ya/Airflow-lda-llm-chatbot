import numpy as np
import pandas as pd
import glob
import json
import random
import os
import datetime as dt
from dotenv import load_dotenv
import os
import sys
load_dotenv('./config/.env')
# save the embeddings in a DB that is persistent
from langchain.document_loaders import TextLoader,PyPDFLoader,JSONLoader
from langchain.schema.document import Document
from langchain.text_splitter import CharacterTextSplitter, RecursiveCharacterTextSplitter
from langchain.vectorstores import FAISS
from langchain.embeddings.cohere import CohereEmbeddings

DATA_DIRECTORY = "./dags/services/data/"
VECTOR_STORE_PATH = "./dags/services/vector_store_folder"
FILE_SEPERATORS = "***********************"


def process_lda_files_for_vector_store(directory_path=DATA_DIRECTORY,file_seperator=FILE_SEPERATORS):
    # Find all files starting with 'lda' in the specified directory
    lda_files = glob.glob(os.path.join(directory_path, 'lda*'))

    # Read content from each file, prepend with filename, and join together
    combined_content = ""
    for file_path in lda_files:
        file_name = os.path.basename(file_path)
        content = pd.read_csv(file_path)
        content = " , ".join(content["Word"])
        combined_content += f"This is scopus topic keywords from {file_name}: {content.strip()}" + file_seperator  # Add filename and content with punctuation
        print("Get all lda datata for vectoe db successfully!")
    return combined_content

def process_data(data_directory=DATA_DIRECTORY,separators=FILE_SEPERATORS):
    #process data
    data = process_lda_files_for_vector_store(data_directory,separators) #make lda result in readable format for llm first
    loader = Document(page_content=data , metadata={"source": "scopus_title"})
    docs = [loader.page_content]
    
    text_splitter = RecursiveCharacterTextSplitter(
    chunk_size = 512, #we can test this
    chunk_overlap  = 0,
    length_function = len,
    separators = separators
)
    docs = text_splitter.create_documents(docs)
    texts = [doc.page_content for doc in docs if doc.page_content.strip("*") != '']

    return texts


def create_vector_store(vector_store_path=VECTOR_STORE_PATH) :   #(start_date,end_date,query,article_result_num,api_data,vector_store_path):
    texts = process_data()
    embeddings = CohereEmbeddings(model = "embed-multilingual-v2.0")
    vector_store = FAISS.from_texts(texts, embeddings) #,metadatas=metadata)  #, location=":memory:",metadatas=metadata, distance_func="Dot")
    retriever=vector_store.as_retriever() # Use for search
    vector_store.save_local(vector_store_path)
    return retriever


def load_vector(vector_store_path=VECTOR_STORE_PATH):
    if os.path.exists(vector_store_path):
        vector_store = FAISS.load_local(
            vector_store_path,
        CohereEmbeddings(model = "embed-multilingual-v2.0")
    )
        return vector_store
    else:
        print(f"Missing files. Upload index.faiss and index.pkl files to {vector_store} directory first")

# def update_vector_store(new_texts:str,vector_store_path=VECTOR_STORE_PATH):
#     vector_store = load_vector(vector_store_path)
#     vector_store.add_texts(new_texts)
#     vector_store.save_local(vector_store_path)

def search(vector_store,query):
    search_result = vector_store.similarity_search_with_score(query)
    return search_result



if __name__ == "__main__":
    #print(create_vector_store('1','2','3','4','5','./vector_store'))
    #print(update_data("abc","./vector_store"))
    # vector_store = load_vector("./dags/services/vector_store_folder")
    # print(search(vector_store,"a"))
    print(glob.glob(os.path.join(DATA_DIRECTORY, 'lda*')))


