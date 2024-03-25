from utils import InsiderScraper, Database, config
from alpaca.data import StockHistoricalDataClient
import time


## TOOK 20 MINS
# Get all tickers / dates from database

# For each day - call get_stock_price and store in new table in database
    # Check if stock price exists for that day - if not store in data base

scraper = InsiderScraper()
db = Database()

# Fetching list of tickers for each data - this way we only have to do one requests per date
    # Alpaca data only starts in 2016
print("Getting all existing dates and tickers\n")
q = """
select
    EXTRACT(DATE FROM filing_date), 
    ARRAY_AGG(ticker) from `open_insider.trades_bronze` 
where EXTRACT(DATE FROM filing_date) >= '2016-01-01' 
group by EXTRACT(DATE FROM filing_date)
"""
ticker_dates = db.query(q)

# Truncating table
# print("TRUNCATING TABLE")
# resp = db.query("truncate table `open_insider.ticker_data`")
# print(resp.total_rows)

# Looping through date, list of tickers and adding info to db
stock_client = StockHistoricalDataClient(config.ALPACA_KEY,  config.ALPACA_SECRET)
cnt = 0
for row in ticker_dates:
    date = row[0]
    tickers = list(set(row[1]))
    # print("Date", date) ###########

    # Fetching stock data
    try:
        stock_data = scraper.get_stock_price(stock_client, tickers=tickers, input_date=date)
    except:
        print(f"SKIPPING {date}\n")
        continue

    # Caching all data to ticker_data table
    try:
        db.cache_new_stock(stock_dic=stock_data)
    except Exception as e:
        print(f"message: {e}")
        print(date, tickers)


    # Tracking how many requests because can only do 200 / min - sleeping for 60 sec after 199
    cnt += 1
    if cnt > 199:
        cnt = 0
        print(f"Done up to {date}")
        print("Sleeping...\n")
        time.sleep(61)