from utils import Database, CloudStorage

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

sto.close_file()
