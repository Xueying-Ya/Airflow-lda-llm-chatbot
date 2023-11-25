FROM apache/airflow:2.7.1-python3.10

# ENV AIRFLOW_HOME ~/airflow

# COPY requirements.txt .

# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install spacy==3.7.2

# RUN pip install spacy-legacy==3.0.12
# RUN pip install spacy-loggers==1.0.5

RUN pip install nltk==3.8.1

RUN python -c "import nltk; nltk.download('punkt')"

RUN python -c "import nltk; nltk.download('stopwords')"

RUN python -m spacy download en_core_web_sm

# EXPOSE 8080

# CMD airflow standalone


