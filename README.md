# Insider Trading Replication

## About
The site <a href="http://openinsider.com/">openinsider.com</a> tracks and publishes all insider trades.

I wanted to know two things with this information:
1. Are these people are making money off these trades?
2. Can I make money knowing these trades?

This project replicates the trades published to openinsider.com to an Alpaca paper trading account.

## Importance of Data Engineering
While I wanted to jump into these questions right away, I knew most of my time was going to be spent building a reliable system to process and prepare the data.
For any data related project, it is generally known around 70-80% of the time and resources are going to be spend on data engeering related tasks. (<a href="https://bennettdatascience.com/tech-tuesdays-why-a-shocking-80-of-data-science-projects-goes-to-data-cleanup/">This</a> blog goes into more detail.)

Without a solid data foundation, you cannot be confident in the analysis built on top of it. 'Garbage in, garbage out' is how I like to think of this concept.
The picture below does a good job of visualizing this concept. If your base data engineering layers are not solid, then everything built on top of that is not worth anything.

<img src="https://github.com/nruffini32/openinsider/assets/71286321/8a14f054-7a9e-48d3-880f-7b16fc82cf9b" width="550"/>

## Data Pipeline
A high level overview of the data pipeline is as follow:
1. Data is ingested from openinsider.com to trades_bronze table (staging_trades is also created with newly processed trades)
2. Simple tranformations are applied to trades_bronze to create trades table
3. New stock data is added to ticker_data 
4. Current stock data is updated in recent_ticker_data
5. Views are created from trades and ticker_data
6. New trades are replicated in alpaca paper trading account and my_orders table is updated with sucessful trades
<img width="1310" alt="image" src="https://github.com/nruffini32/openinsider/assets/71286321/1c8e0756-b851-4cef-b8b1-ca8f3623bc96">

## Data Objects
Schema details can be found [here](docs/schemas.md)
### Tables
trades_bronze: Raw trades from openinsider.com

staging_trades: Recently processed trades - used in downstream scripts. Deleted at the end of pipeline execution

trades: Subset of trades_bronze with applied transformations

ticker_data: Stock market data for all stocks at all dates they were traded at

recent_ticker_data: Current stock market data for all stocks

my_orders: All order that have been placed in Alpaca paper trading account

### Views
**trades_ticker_data**: Joining trades and ticker_data to get ticker data for each trade

**trades_per_insider**: Grouping all trades together per insider

## Technologies
Google Cloud Platform is used for all cloud services
- BigQuery is used as data warehouse
- Cloud Storage is used for logging
- Cloud run is used to schedule scripts daily (defined in Procfile)
  
<a href="https://app.alpaca.markets/brokerage/dashboard/overview">Alpaca</a> paper trading account and trading API is used to replicate trades

## Scripts
The scripts are executed daily (via Cloud Run) in the order they are numbered:
1. 1-trades-bronze.py
2. 1b-trades.py
3. 2a-tickers.py
4. 2b-new-ticker-data.py
5. 4-make-trades.py

[1-trades-bronze.py](1-trades-bronze.py)- Get new trades from openinsider.com and store in trades_bronze tab
- Scrape all trades for current month-year from openinsider.com
- Insert trades into trades_bronze if primary key doesn't exists (filing_date, ticker, insider_name, trade_type)
  - In order to not compare against every record, the script is just comparing against records of the current month-year
- Creating staging_trades table with all new trades
  
[1b-trades.py](1b-trades.py) - Apply transformations from trades_bronze to get wanted data / columns
- Use CTAS statement (with or replace) to create trades table with neccassary filters and columns
  - Doing this instead of some kind of append only logic for two reasons:
      1. Adding logic allows for more room for error - create or replace is guaranteed to have the most recent and accurate data
      2. The create or replace logic does not take enough time to warrant a replacement (takes <10 seconds)
   
[2a-tickers.py](2a-tickers.py) - Adds new trades to ticker_data
- Compares existing max_date in ticker_data to yesterday
  - Since alpaca market api only works for historical data - using yesterday's stock data as most recent
- If max_date is less than yesterday - then we want to process everyday from (max_date, yesterday]
- For each date, we get all distinct tickers that were traded on that date from trades_bronze, and store in ticker_data

[2b-new-ticker-data.py](2b-new-ticker-data.py) - Get all of the most recent stock data for all tickers
- Get all distinct tickers from ticker_data
- For each ticker, get most recent stock data (yesterday)
- Recreate recent_ticker_data with this information

[4-make-trades.py](4-make-trades.py) - Replicate all trades from openinsider.com to Alpaca paper trading account
- Fetches all trades from staging table
- If the trades was a purchase
  - Calculate how much I am buying by following the following steps:
    - Calculate how much the insider bought compared to their total shares (result is a percentage)
    - Buy X dollars worth of the stock buy using that percentage (percentage * hardcoded number - currently 1000)
- If the trades was a sale
  - If I do not currently own any of the stock then skip
  - If I do own the stock, then need to calculate how much I want to sell
    - This calculation follows a similar process
    - Instead of finding a percentage of the hard coded value, I sell the same percentage of my shares that the insider sold
- Store the order in my_orders
- Delete staging table

#### Modules
There are three main modules that are used through the scripts

[utils.database.Database](utils/database.py) - Interface to interact with BigQuery data warehouse

[utils.open_insider.InsiderScraper](utils/open_insider.py)- Interface to scrape data from openinsider.com
  
[utils.storage.CloudStorage](utils/storage.py) - Interface to interact with GCS - mainly use for logging
  
###### Took a lot of scraper module inspiration from here - https://github.com/sd3v/openinsiderData

## The Fun Part
After the bitch work is done I can finally use the data for something cool.
### Analysis
- Go through year by year analysis
- Go through total analysis
- Compare against S&P 500
- Maybe just think to notebook
### Alpaca Paper Trading Account
- Knowing this - I just decided to replicate every trade
- Started 03/25/2024
- Will potentially work on way to update (either web app or twitter bot)

Thanks for reading!
