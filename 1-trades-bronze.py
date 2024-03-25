from datetime import datetime
from utils import InsiderScraper, Database, CloudStorage

print("Starting trades script")

## Run this every day to process new trades
    # FULL_LOAD - depends if you want to check against existing records or not - False = it is checking
    # START_YEAR - what year you want to process from
    # START_MONTH - will process current month and last month with how it is configured now
current_year = datetime.now().year
current_month = datetime.now().month

FULL_LOAD = False
START_YEAR = 2024
START_MONTH = current_month
# START_MONTH = current_month - 1 if current_month != 1 else 12
STAGING_TABLE = "staging_trades"

scraper = InsiderScraper()
db = Database()
sto = CloudStorage("1_trades_bronze")

for year in range(START_YEAR, current_year + 1):
    # Jan and Feb missing for 2013 for some reason - end_month is current month for this year
    start_month = START_MONTH if year != 2013 else 3
    end_month = current_month if year == current_year else 12
    for month in range(start_month, end_month + 1):

        # Get trades for each month-year
        trades = scraper.get_trades_for_month(month, year)
        # sto.print(f"Total trades for {month}-{year}: {len(trades)}")

        # Put trades into BigQuery table 'trades_bronze' - if FULL_LOAD is false it will check against existing records and only insert new ones
        added_trades = db.cache_trades(trades=trades, full_load=FULL_LOAD)

        # Putting recent trades into new table to add them to OLAP table
        if len(added_trades) != 0:
            sto.print(f"Adding {len(added_trades)} to {STAGING_TABLE}")
            db.create_table_from_trades(STAGING_TABLE, added_trades)
        else:
            sto.print("No trades added to staging table")

        num_rows = db.get_num_rows(STAGING_TABLE)
        sto.print(f"{num_rows} total rows in {STAGING_TABLE}")
        

# MIGHT JUST WANT TO RUN PURCHASES AND SALES SCRIPTS HERE
    # HAVE THEM OUT FOR DEBUGGING PURPOSES
            

sto.close_file()