from utils import Database, CloudStorage

# Script to create trades table from trades_bronze
  # Grab column that will be used for analyasis
  # Would do this in a better way but only takes a couple seconds anyway

db = Database()
sto = CloudStorage("1b_trades")

sto.print("Creating 'trades' table from trades_bronze")
q = """
create or replace table `open_insider.trades` as 
select
  filing_date,
  ticker,
  insider_name,
  title,
  trade_type,
  price,
  qty,
  owned,
  value,
from
  `open_insider.trades_bronze`
where
  trade_type in ("P - Purchase", "S - Sale") and
  qty != 0 and
  extract(year from filing_date) >= 2016
"""

db.query(q)

# Have to run this so logging file is actually created
sto.close_file()
