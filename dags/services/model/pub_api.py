import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv('./config/.env')

scopus_api_key = os.environ.get('SCOPUS_API_KEY')
DATA_PATH = "./data"


def process_original_scopus_data(file, sjr_dic): #provided data
    """
    Data contains
    1. publication_year
    2. journal_name
    3. title
    4. cited_count
    5. country
    6. university
    7. author
    8. tier
    """

    with open(file) as f:
        data = json.load(f)

    university_data = data['abstracts-retrieval-response']['item']['bibrecord']['head']['author-group']['affiliation']['organization']
    university = university_data[1]['$'] if len(university_data) > 1 else (university_data[0]['$'] if len(university_data) == 1 else None)

    journal_name = data['abstracts-retrieval-response']['item']['bibrecord']['head']['source'].get('sourcetitle')
    publication_year = data['abstracts-retrieval-response']['item']['bibrecord']['head']['source']['publicationyear'].get('@first')
    cited_count = data['abstracts-retrieval-response']['coredata'].get('citedby-count')
    country = data['abstracts-retrieval-response']['item']['bibrecord']['head']['author-group']['affiliation'].get('country')
    author = data['abstracts-retrieval-response']['coredata']['dc:creator']['author'][0].get('ce:given-name')

    journal_title = data['abstracts-retrieval-response']['item']['bibrecord']['head']['source'].get('sourcetitle')
    tier = sjr_dic.get(journal_title)

    return {
        "publication_year": publication_year,
        "journal_name": journal_name,
        "title": data['abstracts-retrieval-response']['coredata'].get('dc:title'),
        "cited_count": cited_count,
        "country": country,
        "university": university if university else None,
        "author": author if author else None,
        "tier": tier if tier else None,
    }


def retrieve_new_articles(query, sjr_dic, apikey, result_number=1000):
    """
    Data contains
    1. publication_year
    2. journal_name
    3. title
    4. cited_count
    5. country
    6. university
    7. author
    8. tier

    Example query -> By year query = "SOCI OR DENT OR DECI OR PHYS OR CENG OR EART OR VETE OR PHAR OR ENGI OR ECON OR ARTS OR IMMU OR MULT OR MATH OR NEUR OR PSYC OR AGRI OR BUSI OR NURS OR CHEM OR COMP OR ENVI OR MATE OR MEDI OR BIOC OR ENER OR HEAL 
    
    PUBYEAR > 2022 AND PUBYEAR < 2024"

    -> By date = B AND PUBDATETXT(20-November-2022)


    """
    result_lst = []
    total_articles = 0

    while total_articles < result_number:
        try:
          scopus = requests.get(f'https://api.elsevier.com/content/search/scopus?query={query}&count=25&start={total_articles}&apiKey={apikey}')

        except Exception as e:
          print(e)
        scopus_dic = scopus.json()

        entries = scopus_dic.get('search-results', {}).get('entry', [])

        if not entries:
          data = pd.DataFrame(result_lst)
          data.to_csv('./dags/services/data/new_data.csv')
          return result_lst

        for item in entries:
            result = {
                "publication_year": item.get('prism:coverDate')[:4] if item.get('prism:coverDate') else None,
                "journal_name": item.get('prism:publicationName'),
                "title": item.get('dc:title'),
                "cited_count": item.get('citedby-count'),
                "country": item['affiliation'][0]['affiliation-country'] if 'affiliation' in item and item['affiliation'] else None,
                "university": item['affiliation'][0]['affilname'] if 'affiliation' in item and item['affiliation'] else None,
                "author": item.get('dc:creator'),
                "tier": sjr_dic.get(item.get('prism:publicationName').lower()),
            }

            result_lst.append(result)
            total_articles += 1

            if total_articles >= result_number:
                data = pd.DataFrame(result_lst)
                data.to_csv('./dags/services/data/new_data.csv')
                return result_lst


if __name__ == "__main__":
  with open('./dags/services/sjr_data.json') as f:
    sjr_dic = json.load(f)

  current_date = datetime.now()

  # Format the date as '20-November-2022'
  formatted_date = current_date.strftime('%d-%B-%Y')

  query = f"SOCI OR DENT OR DECI OR PHYS OR CENG OR EART OR VETE OR PHAR OR ENGI OR ECON OR ARTS OR IMMU OR MULT OR MATH OR NEUR OR PSYC OR AGRI OR BUSI OR NURS OR CHEM OR COMP OR ENVI OR MATE OR MEDI OR BIOC OR ENER OR HEAL AND PUBDATETXT({formatted_date})"
  print(retrieve_new_articles(query,sjr_dic,scopus_api_key,result_number=10))


