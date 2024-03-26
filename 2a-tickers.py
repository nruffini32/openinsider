from utils import InsiderScraper, Database, config, CloudStorage
from alpaca.data import StockHistoricalDataClient
import time
from datetime import datetime, timedelta

# Script to get the price of tickers for each day it is traded
    # On a daily run will only get tickers from the max date in the database up until yesterday

print("Starting tickers script")

scraper = InsiderScraper()
db = Database()
sto = CloudStorage("2a_tickers")

# Function to add tickers to database
def add_tickers_to_db(ticker_dates):
    cnt = 0
    for row in ticker_dates:
        date = row[0]
        tickers = list(set(row[1]))
        # sto.print("Date", date) ###########

        # Fetching stock data
        try:
            stock_data = scraper.get_stock_price(stock_client, tickers=tickers, input_date=date)
        except Exception as e:
            sto.print(f"SKIPPING {date}: {e}\n")
            continue

        # Caching all data to ticker_data table
        try:
            db.cache_new_stock(stock_dic=stock_data)
        except Exception as e:
            sto.print(f"message: {e}")
            sto.print(date, tickers)

        sto.print(f"Added data for {len(tickers)} tickers")

        # Tracking how many requests because can only do 200 / min - sleeping for 60 sec after 199
        cnt += 1
        if cnt > 199:
            cnt = 0
            sto.print(f"Done up to {date}")
            sto.print("Sleeping...\n")
            time.sleep(61)


# Get today's date
today = datetime.now().date()
yesterday = today - timedelta(days=1)

# Get max data from ticker data
data = db.query("select max(date) from `open_insider.ticker_data`")
for d in data:
    max_date = d.values()[0]

sto.print(f"Max date: {max_date} - Yesterday: {yesterday}\n")

if max_date == yesterday:
    sto.print("No new trades - exiting script")
    exit()

stock_client = StockHistoricalDataClient(config.ALPACA_KEY,  config.ALPACA_SECRET)

# Need to add tickers for dates from (max_date -> yesterday) - not including max_date because they have already been processed
cnt = 0
date_difference = yesterday - max_date
for day in range(date_difference.days):
    d = max_date + timedelta(day+1)
    if d.weekday() >= 5:
        sto.print("Skipping weekend")
        continue
    sto.print(f"Updating ticker_data for: {d}")

    q = f"""
    select 
        EXTRACT(DATE FROM filing_date),
        ARRAY_AGG(ticker) 
    from `open_insider.trades_bronze` 
    where EXTRACT(DATE FROM filing_date) = '{d}' 
    group by EXTRACT(DATE FROM filing_date)
    """
    query_resp = db.query(q)

    add_tickers_to_db(query_resp)

# Have to run this so logging file is actually created
sto.close_file()