import json
import os
import re
import pandas as pd
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

TABLE_CREATION_QUERIES = [
    """CREATE TABLE IF NOT EXISTS job (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(225),
        industry VARCHAR(225),
        description TEXT,
        employment_type VARCHAR(125),
        date_posted DATE
    )""",
    """CREATE TABLE IF NOT EXISTS company (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        name VARCHAR(225),
        link TEXT,
        FOREIGN KEY (job_id) REFERENCES job(id)
    )""",
    """CREATE TABLE IF NOT EXISTS education (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        required_credential VARCHAR(225),
        FOREIGN KEY (job_id) REFERENCES job(id)
    )""",
    """CREATE TABLE IF NOT EXISTS experience (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        months_of_experience INTEGER,
        seniority_level VARCHAR(25),
        FOREIGN KEY (job_id) REFERENCES job(id)
    )""",
    """CREATE TABLE IF NOT EXISTS salary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        currency VARCHAR(3),
        min_value NUMERIC,
        max_value NUMERIC,
        unit VARCHAR(12),
        FOREIGN KEY (job_id) REFERENCES job(id)
    )""",
    """CREATE TABLE IF NOT EXISTS location (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        country VARCHAR(60),
        locality VARCHAR(60),
        region VARCHAR(60),
        postal_code VARCHAR(25),
        street_address VARCHAR(225),
        latitude NUMERIC,
        longitude NUMERIC,
        FOREIGN KEY (job_id) REFERENCES job(id)
    )"""
]

@task()
def create_tables():
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    for query in TABLE_CREATION_QUERIES:
        sqlite_hook.run(query)

@task()
def extract():
    source_file_path = 'source/jobs.csv'
    extracted_dir = 'staging/extracted'
    os.makedirs(extracted_dir, exist_ok=True)
    try:
        df = pd.read_csv(source_file_path)
    except FileNotFoundError:
        print(f"Error: The source file '{source_file_path}' does not exist.")
        return
    for index, row in df.iterrows():
        context = row.get('context', '')
        if not isinstance(context, str):
            context = str(context)
        file_path = os.path.join(extracted_dir, f"job_{index}.txt")
        with open(file_path, 'w') as file:
            file.write(context)
    print(f"Extracted {len(df)} rows and saved as text files in {extracted_dir}")

@task()
def transform():
    extracted_dir = 'staging/extracted'
    transformed_dir = 'staging/transformed'
    os.makedirs(transformed_dir, exist_ok=True)
    for filename in os.listdir(extracted_dir):
        if filename.endswith('.txt'):
            file_path = os.path.join(extracted_dir, filename)
            with open(file_path, 'r') as file:
                context = file.read()
            transformed_data = {
                "job": {
                    "title": "job_title",
                    "industry": "job_industry",
                    "description": clean_description(context),
                    "employment_type": "job_employment_type",
                    "date_posted": "job_date_posted",
                },
                "company": {
                    "name": "company_name",
                    "link": "company_linkedin_link",
                },
                "education": {
                    "required_credential": "job_required_credential",
                },
                "experience": {
                    "months_of_experience": "job_months_of_experience",
                    "seniority_level": "seniority_level",
                },
                "salary": {
                    "currency": "salary_currency",
                    "min_value": "salary_min_value",
                    "max_value": "salary_max_value",
                    "unit": "salary_unit",
                },
                "location": {
                    "country": "country",
                    "locality": "locality",
                    "region": "region",
                    "postal_code": "postal_code",
                    "street_address": "street_address",
                    "latitude": "latitude",
                    "longitude": "longitude",
                },
            }
            transformed_file_path = os.path.join(transformed_dir, f"{filename.replace('.txt', '.json')}")
            with open(transformed_file_path, 'w') as json_file:
                json.dump(transformed_data, json_file)

def clean_description(description):
    return re.sub(r'\s+', ' ', description.strip())

@task()
def load():
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    transformed_dir = 'staging/transformed'
    for file_name in os.listdir(transformed_dir):
        if file_name.endswith(".json"):
            file_path = os.path.join(transformed_dir, file_name)
            with open(file_path, 'r') as file:
                transformed_data = json.load(file)
            job_data = transformed_data['job']
            company_data = transformed_data['company']
            education_data = transformed_data['education']
            experience_data = transformed_data['experience']
            salary_data = transformed_data['salary']
            location_data = transformed_data['location']
            insert_job_query = """
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (:title, :industry, :description, :employment_type, :date_posted)
            """
            sqlite_hook.run(insert_job_query, parameters=job_data)
            get_job_id_query = "SELECT last_insert_rowid()"
            job_id = sqlite_hook.get_first(get_job_id_query)
            insert_company_query = """
            INSERT INTO company (job_id, name, link)
            VALUES (:job_id, :name, :link)
            """
            sqlite_hook.run(insert_company_query, parameters={**company_data, "job_id": job_id})
            insert_education_query = """
            INSERT INTO education (job_id, required_credential)
            VALUES (:job_id, :required_credential)
            """
            sqlite_hook.run(insert_education_query, parameters={**education_data, "job_id": job_id})
            insert_experience_query = """
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES (:job_id, :months_of_experience, :seniority_level)
            """
            sqlite_hook.run(insert_experience_query, parameters={**experience_data, "job_id": job_id})
            insert_salary_query = """
            INSERT INTO salary (job_id, currency, min_value, max_value, unit)
            VALUES (:job_id, :currency, :min_value, :max_value, :unit)
            """
            sqlite_hook.run(insert_salary_query, parameters={**salary_data, "job_id": job_id})
            insert_location_query = """
            INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
            VALUES (:job_id, :country, :locality, :region, :postal_code, :street_address, :latitude, :longitude)
            """
            sqlite_hook.run(insert_location_query, parameters={**location_data, "job_id": job_id})
    print(f"Data loaded successfully into the SQLite database from {transformed_dir}")

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    create_tables_task = create_tables()
    create_tables_task >> extract() >> transform() >> load()

etl_dag()
