import os
import re
import urllib.parse
import logging
import pandas as pd
from pandas import json_normalize
import requests

from dotenv import load_dotenv

from googletrans import Translator
import snowflake.connector
from sqlalchemy import create_engine

translator = Translator()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_linkedin_job_data(rapidapi_key, rapidapi_host):
    logger.info("Starting LinkedIn job data extraction from API...")
    headers = {
        'x-rapidapi-key': rapidapi_key,
        'x-rapidapi-host': rapidapi_host
    }
    location = "Australia"
    limit = 100
    offset = 0
    titles = ["Data Engineer", "Data Scientist", "Data Analyst"]
    df_daily_all = pd.DataFrame()

    for title_filter in titles:
        title_encoded = urllib.parse.quote(title_filter)
        location_encoded = urllib.parse.quote(location)
        base_url = f"/active-jb-24h?limit={limit}&offset={offset}&title_filter={title_encoded}&location_filter={location_encoded}"
        url = f"https://{rapidapi_host}{base_url}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            df_daily = json_normalize(data)
            df_daily['job_category'] = title_filter
            logger.info(f"Fetched '{title_filter}' jobs: {df_daily.shape[0]} rows")
            df_daily_all = pd.concat([df_daily_all, df_daily], ignore_index=True)
        else:
            logger.error(f"API error for '{title_filter}': HTTP {response.status_code}")
    logger.info("Job data extraction completed.")
    return df_daily_all


def update_columns(df_daily_all):
    logger.info("Updating column names and selecting relevant columns...")
    df_daily_all.columns = df_daily_all.columns.str.upper()
    columns_keep = [
        'ID', 'DATE_POSTED', 'DATE_CREATED', 'TITLE', 'JOB_CATEGORY',
        'ORGANIZATION', 'ORGANIZATION_URL', 'DATE_VALIDTHROUGH', 'LOCATIONS_RAW',
        'LOCATION_TYPE', 'LOCATION_REQUIREMENTS_RAW', 'EMPLOYMENT_TYPE', 'URL',
        'SOURCE_TYPE', 'SOURCE', 'SOURCE_DOMAIN', 'ORGANIZATION_LOGO',
        'CITIES_DERIVED', 'REGIONS_DERIVED', 'COUNTRIES_DERIVED',
        'LOCATIONS_DERIVED', 'TIMEZONES_DERIVED', 'LATS_DERIVED', 'LNGS_DERIVED',
        'REMOTE_DERIVED', 'RECRUITER_NAME', 'RECRUITER_TITLE', 'RECRUITER_URL',
        'LINKEDIN_ORG_EMPLOYEES', 'LINKEDIN_ORG_URL', 'LINKEDIN_ORG_SIZE',
        'LINKEDIN_ORG_SLOGAN', 'LINKEDIN_ORG_INDUSTRY', 'LINKEDIN_ORG_FOLLOWERS',
        'LINKEDIN_ORG_HEADQUARTERS', 'LINKEDIN_ORG_TYPE', 'LINKEDIN_ORG_FOUNDEDDATE',
        'LINKEDIN_ORG_SPECIALTIES', 'LINKEDIN_ORG_LOCATIONS', 'LINKEDIN_ORG_DESCRIPTION',
        'LINKEDIN_ORG_RECRUITMENT_AGENCY_DERIVED', 'SENIORITY', 'DIRECTAPPLY',
        'LINKEDIN_ORG_SLUG'
    ]
    selected_columns = [col for col in columns_keep if col in df_daily_all.columns]
    df_daily_all = df_daily_all[selected_columns]
    logger.info("Column update completed.")
    return df_daily_all


def connect_to_snowflake(user, password, account, warehouse, database, schema):
    logger.info("Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        logger.info("Connection to Snowflake established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        return None


def query_raw_api_data(conn, table_name='linkedin_job_api_cleaned_data'):
    logger.info(f"Querying raw API data from table: {table_name} ...")
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    logger.info(f"Retrieved {df.shape[0]} rows from Snowflake.")
    return df

def extract_job_date(df_daily_all):
    logger.info("Extracting job date...")
    df_daily_all['JOB_DATE'] = pd.to_datetime(df_daily_all['DATE_CREATED']).dt.date
    return df_daily_all['JOB_DATE']

def extract_city(text):
    text = str(text)
    city_pattern = r"'addressLocality':\s*'(.*)',\s'addressRegion':"
    match = re.search(city_pattern, text)
    if match:
        city = match.group(1)
        if 'sidney' in city.lower() or 'sídney' in city.lower() or '悉尼' in city.lower():
            return "Sydney"
        return city
    else:
        return None

def extract_state(text):
    text = str(text)
    state_pattern = r"'addressRegion':\s*(.*)',\s'streetAddress'"
    match = re.search(state_pattern, text)
    if match:
        state = match.group(1).replace("'", "").strip()
        return state
    else:
        return None

def extract_employment_type(df_daily_all):
    logger.info("Extracting employment type...")
    df_daily_all['EMPLOYMENT_TYPE'] = (
        df_daily_all['EMPLOYMENT_TYPE']
        .astype(str)
        .str.replace(r"[\[\]']", '', regex=True)
        .str.strip()
    )
    return df_daily_all['EMPLOYMENT_TYPE']

def extract_employee_size(df_daily_all):
    logger.info("Extracting organization employee size...")
    df_daily_all['ORG_SIZE'] = (
        df_daily_all['LINKEDIN_ORG_SIZE']
        .astype(str)
        .str.replace(r"employees", '', regex=True)
        .str.strip()
    )
    return df_daily_all['ORG_SIZE']

def merge_duplicates(df_old, df_new):
    logger.info("Merging new and existing data, removing duplicates...")
    merged_df = pd.concat([df_old, df_new], axis=0)
    merged_df.drop_duplicates(subset=['ID'], keep='last', inplace=True)
    merged_df.reset_index(drop=True, inplace=True)
    logger.info("Merge complete.")
    return merged_df

def translate_text(text, target_language='en'):
    try:
        if not text or pd.isna(text) or str(text).strip().lower() in ['nan', 'na']:
            logger.warning("Empty or invalid text provided for translation. Returning 'NA'.")
            return 'NA'
        translated_text = translator.translate(text, dest=target_language)
        logger.info(f"Translated '{text}' to '{translated_text.text}'")
        return translated_text.text
    except Exception as e:
        logger.error(f"Error translating text '{text}': {e}")
        return text

def load_to_snowflake(df_merged, user, password, account, warehouse, database, schema, table_name):
    logger.info(f"Loading processed data to Snowflake table: {table_name} ...")
    engine = create_engine(
        f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'
    )
    df_merged.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False
    )
    logger.info(f"Data loaded to Snowflake table {table_name} successfully.")


def main():
    # 1. Load environment variables
    load_dotenv()
    rapidapi_key = os.getenv('RAPIDAPI_KEY')
    rapidapi_host = "linkedin-job-search-api.p.rapidapi.com"
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
    user = "NIKKILW2025"
    account = "gbszkwp-by30611"
    warehouse = "SNOWFLAKE_LEARNING_WH"
    database = "linkedin_db"
    schema = "linkedin_raw"

    # 2. Fetch LinkedIn data from API
    df_daily_all = extract_linkedin_job_data(rapidapi_key, rapidapi_host)
    df_daily_all = update_columns(df_daily_all)

    # 3. Optionally save to local CSV for backup
    df_daily_all.to_csv('linkedin_jobs_daily.csv', index=False)
    logger.info("Saved scraped data to linkedin_jobs_daily.csv.")

    # 4. Connect to Snowflake and get previous data
    conn = connect_to_snowflake(user, snowflake_password, account, warehouse, database, schema)
    if conn is None:
        logger.error("Could not connect to Snowflake. Exiting pipeline.")
        return

    df_old = query_raw_api_data(conn)
    conn.close()

    # 5. Field extraction
    df_daily_all['JOB_DATE'] = extract_job_date(df_daily_all)
    df_daily_all['CITY'] = df_daily_all['LOCATIONS_RAW'].apply(extract_city)
    df_daily_all['STATE'] = df_daily_all['LOCATIONS_RAW'].apply(extract_state)
    df_daily_all['EMPLOYMENT_TYPE'] = extract_employment_type(df_daily_all)
    df_daily_all['ORG_SIZE'] = extract_employee_size(df_daily_all)

    # 6. Retain only relevant columns
    needed_cols = [
        'ID', 'TITLE', 'JOB_CATEGORY', 'JOB_DATE', 'CITY', 'STATE', 'EMPLOYMENT_TYPE',
        'ORGANIZATION', 'ORGANIZATION_URL', 'URL', 'SOURCE_TYPE', 'SOURCE',
        'SOURCE_DOMAIN', 'ORGANIZATION_LOGO', 'REMOTE_DERIVED', 'RECRUITER_NAME',
        'RECRUITER_TITLE', 'RECRUITER_URL', 'LINKEDIN_ORG_URL', 'ORG_SIZE',
        'LINKEDIN_ORG_INDUSTRY', 'LINKEDIN_ORG_HEADQUARTERS', 'LINKEDIN_ORG_TYPE',
        'LINKEDIN_ORG_FOUNDEDDATE', 'LINKEDIN_ORG_SPECIALTIES',
        'LINKEDIN_ORG_LOCATIONS', 'LINKEDIN_ORG_DESCRIPTION',
        'LINKEDIN_ORG_RECRUITMENT_AGENCY_DERIVED', 'SENIORITY', 'DIRECTAPPLY',
        'LINKEDIN_ORG_SLUG'
    ]
    df_daily_all = df_daily_all[needed_cols]

    # 7. Remove duplicates and prepare for upload
    df_merged = merge_duplicates(df_old, df_daily_all)

    #8. Translate business name, city, state and seniority to English

    df_merged['CITY'] = df_merged['CITY'].apply(lambda x: translate_text(x, target_language='en'))

    df_merged['STATE'] = df_merged['STATE'].apply(lambda x: translate_text(x, target_language='en'))

    df_merged['ORGANIZATION'] = df_merged['ORGANIZATION'].apply(lambda x: translate_text(x, target_language='en'))

    df_merged['SENIORITY'] = df_merged['SENIORITY'].apply(lambda x: translate_text(x, target_language='en'))



    # 9. Load processed data back to Snowflake
    load_to_snowflake(df_merged, user, snowflake_password, account, warehouse, database, schema, 'linkedin_job_api_cleaned_data')
    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()
