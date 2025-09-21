import requests, time, pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
LIMIT = 1000


def run_stock_job():
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    tickers = []
    while url:
        response = requests.get(url)
        data = response.json()

        # handle rate limit errors gracefully
        if "error" in data:
            print("Error:", data["error"])
            print("Waiting 60s before retry...")
            time.sleep(60)
            continue  # retry same url

        # extend tickers if results exist
        results = data.get("results", [])
        tickers.extend(results)
        print(f"Collected {len(tickers)} tickers so far")

        # move to next page (may be None at the end)
        url = data.get("next_url")
        if url:  # polygon requires apiKey again
            url += f"&apiKey={POLYGON_API_KEY}"

        # respect free-tier rate limit
        time.sleep(12)

    print(f"Done! Total tickers collected: {len(tickers)}")

    # ---- SAVE TO CSV ----
    df = pd.DataFrame(tickers)
    df.to_csv("tickers.csv", index=False)
    print("Saved to tickers.csv")

if __name__ == '__main__':
    run_stock_job()