import os
from datetime import date
import time
import requests
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# --- Load environment variables ---
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000
DS = date.today() #datetime.now().strftime('%Y-%m-%d')

SNOWFLAKE_USERNAME = os.getenv("SNOWFLAKE_USERNAME")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = "STOCK_TICKERS"  


def run_stock_job():
    url = (
        f"https://api.polygon.io/v3/reference/tickers"
        f"?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    )
    tickers = []

    while url:
        response = requests.get(url)
        data = response.json()

        # Handle rate limit or error
        if "error" in data:
            print("Error:", data["error"])
            print("Waiting 60s before retry...")
            time.sleep(60)
            continue

        # Append results
        results = data.get("results", [])
        tickers.extend(results)
        print(f"Collected {len(tickers)} tickers so far...")

        # Next page
        url = data.get("next_url")
        if url:
            url += f"&apiKey={POLYGON_API_KEY}"

        time.sleep(12)  # respect free-tier rate limit

    print(f"✅ Done! Total tickers collected: {len(tickers)}")

    # Create DataFrame
    df = pd.DataFrame(tickers)
    df['ds'] = DS
    df.columns = [col.upper() for col in df.columns]
    return df


def load_to_snowflake(df: pd.DataFrame):
    # --- Connect to Snowflake ---
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USERNAME,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    # --- Write DataFrame to Snowflake ---
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        SNOWFLAKE_TABLE,
    )

    if success:
        print(f"✅ Successfully wrote {nrows} rows into {SNOWFLAKE_TABLE}")
    else:
        print("❌ Failed to write data to Snowflake")

    conn.close()


if __name__ == "__main__":
    df = run_stock_job()
    load_to_snowflake(df)
