# openinsider.com

## About
The site <a href="http://openinsider.com/">openinsider.com</a> tracks and publishes all insider trades.

I wanted to know two things with this information:
1. Are these people are making money off these trades?
2. Can I make money knowing these trades?

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

#### Data objects
trades_bronze: Stores all raw trades from open_insider
trades: Same as trades_bronze with simple transformations
ticker_data: Stock market data (open, high, low, close) for all stocks at all dates they were traded at
recent_ticker_data: Current stock market data for all stocks
my_order: 

#### Technologies
Google Cloud Platform is used for all cloud services
- BigQuery is used as data warehouse
- Cloud Storage is used for logging
- Cloud run is used to schedule scripts daily
<a href="https://app.alpaca.markets/brokerage/dashboard/overview">Alpaca</a> paper trading account and trading API is used to replicate trades

- explain how pipeline works
- High level overview of each script




### Tables


#### Modules
- Describe each module all three of them and there basic functions

- Took a lot of scraper module inspiration from here
https://github.com/sd3v/openinsiderData

#### Scheduling
- Using cloud run
- How it is being run every day


## How I solved these questions.
- Finally when I finished all the

After the bitch work is done I can finally use the data for something cool. Explain analysis - each year they gained XXx over last 6 they gained 216% compares to S&P 500 which gained.
Have alpaca papaer trading account that is ripping. Will keep it updated.


