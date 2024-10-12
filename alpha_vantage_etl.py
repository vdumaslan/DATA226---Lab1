from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


from datetime import timedelta
from datetime import datetime
import requests
import logging

# Initialize the logger
logger = logging.getLogger(__name__)

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

def create_table(table, cur):
  try:
    cur.execute(f"""
    create or replace table {table} (
      symbol string,
      date date,
      open float,
      high float,
      low float,
      close float,
      volume float,
      primary key (symbol, date)
    );
    """)
  except Exception as e:
    logger.error(f"Error creating table: {e}")
    raise Exception("Error creating table")

def insert_records(table, data, cur):
    try:
        for d in data:
            open_price = d["1. open"]
            high = d["2. high"]
            low = d["3. low"]
            close = d["4. close"]
            volume = d["5. volume"]
            date = d["date"]
            symbol = d["symbol"]

            sql = f"""
                INSERT INTO {table} (symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            try:
                cur.execute(sql, (symbol, date, open_price, high, low, close, volume))
                logger.info(f"Inserted record for {date}")
            except Exception as e:
                logger.error(f"Error inserting record: {e}")
                raise
        logger.info(f"Inserted records successfully")
    except Exception as e:
        logger.error(f"Transaction failed: {e}")
        raise

def extract(symbol, api_key):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    r = requests.get(url)
    data = r.json()
    return data

def transform(data, symbol):
    results = []   # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)
    for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
        stock_info = data["Time Series (Daily)"][d]
        stock_info["date"] = d
        stock_info["symbol"] = symbol
        results.append(stock_info)
    return results[:90]

@task
def load(data, target_table):

    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        create_table(target_table, cur)
        insert_records(target_table, data, cur)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
    finally:
        cur.close()

@task
def combine_et(symbols, api_key):       # Used to combine multiple symbols into one table (calls extract and transform)
    combined_results = []
    for symbol in symbols:
        data = extract(symbol, api_key)
        transformed_data = transform(data, symbol)
        combined_results.extend(transformed_data)  # Combine the transformed data
    return combined_results

with DAG(
    dag_id = 'AlphaVantage_ETL_Lab1',
    start_date = datetime(2024,9,27),
    catchup=False,
    tags=['ETL'],
    schedule_interval='0 12 * * *',
) as dag:
    symbols = ["AAPL", "NVDA"]
    target_table = "raw_data.time_series_daily"
    api_key = Variable.get("vantage_api_key")

    combined_data = combine_et(symbols, api_key)
    load_data = load(combined_data, target_table)

    combined_data >> load_data

    logger.info("ETL process completed")