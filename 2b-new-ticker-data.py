from utils import Database, InsiderScraper, config, CloudStorage
from alpaca.data import StockHistoricalDataClient
import datetime

# Script to get most recent price for all tickers

db = Database()
scraper = InsiderScraper()
sto = CloudStorage("2b_new_tickers")

# Getting all tickers that have been traded and putting them in a list
sto.print("\nStarting most recent ticker data script")
q = """select
distinct ticker
from `open_insider.ticker_data`"""
rows = db.query(q)
sto.print("Getting all tickers in og ticker table")
tickers = []
for row in rows:
    ticker = row.values()[0]
    tickers.append(ticker)

# Getting last weekday
today = datetime.date.today() - datetime.timedelta(1)
# Subtract days until a weekday (Monday=0, Sunday=6) is reached
while today.weekday() >= 5:  # 5 and 6 represent Saturday and Sunday
    today -= datetime.timedelta(days=1)
yesterday = today.strftime("%Y-%m-%d")

# Getting stock data and storing in a table
TABLE_NAME = "recent_ticker_data"

sto.print("Fetching most recent data from api")
stock_client = StockHistoricalDataClient(config.ALPACA_KEY,  config.ALPACA_SECRET)
ticker_data = scraper.get_stock_price(stock_client, tickers, yesterday)

sto.print(f"Dropping and recreating {TABLE_NAME}")
db.drop_table(TABLE_NAME)
db.cache_new_stock(ticker_data, "recent_ticker_data")

# Have to run this so logging file is actually created
sto.close_file()
