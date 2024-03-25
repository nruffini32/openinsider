from utils import Database

db = Database()
client = db.get_client()

### NEED TO FIX QUERY BELOW  ##############
    # Currently broken - if someone has only purchases or only sales they do not show up in the table
    # Can either store them as seperate
    # Combing all trades into one
    # Need to recreate trades_per_insider whenebver I decide

# KEY = (ticker, insider_name)
# Merge staging_trades into trades_per_insider

# Loop through each row in staging_trades
    # if it is not a purchase or a sales - skip it

    # If key exists
        # increment either num_purchases or num_sales based on trade_type
        # Add trade data to either purchasese or sales based on trade type
        # ARRAY_AGG  ?

    # If key does not exists
        # Just add the whole record in grouped format

# Delete staging_trades and staging_trades_grouped


STAGING_TABLE = "staging_trades"

q = f"""
with sales_grouped as (
  select
    ticker,
    insider_name,
    count(*) as num_sales,
    ARRAY_AGG(
        STRUCT(
            trade_type AS type,
            trade_date AS date,
            price,
            qty,
            owned )
        ) AS sales
    from `open_insider.{STAGING_TABLE}`
    where trade_type like '%Sale%'
  group by ticker, insider_name
),
purchases_grouped as (
    select
    ticker,
    insider_name,
    count(*) as num_purchases,
    ARRAY_AGG(
        STRUCT(
            trade_type AS type,
            trade_date AS date,
            price,
            qty,
            owned )
        ) AS purchases
    from `open_insider.{STAGING_TABLE}`
    where trade_type = 'P - Purchase'
  group by ticker, insider_name
)

select 
  s.ticker,
  s.insider_name,
  num_purchases,
  purchases,
  num_sales,
  sales
from 
    sales_grouped s
join 
    purchases_grouped p
on 
    s.ticker = p.ticker and
    s.insider_name = p.insider_name
"""
rows = client.query_and_wait(q)
print(rows.num_results)

for row in rows:
    print(row)
