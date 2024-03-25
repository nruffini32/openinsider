from utils import Database

db = Database()

table = "open_insider.sales"
filter = "trade_type = 'S - Sale' or trade_type = 'S - Sale+OE'"

# MIGHT NOT BE BEST WAY TO DO THIS
# Load recent purchases into purchases table (sqlite or Big Query ????)
# print(f"Dropping and creating {table}\n") 
# db.drop_table(table)
query = f"""
    select *
    from `open_insider.trades_bronze`
    where {filter}
"""
db.query(f"create or replace table `{table}` as {query}")