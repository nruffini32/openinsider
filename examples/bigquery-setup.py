# from utils import Database
from google.cloud import bigquery
import os


# db = Database()

# Setting up credentials
# Get the directory of the current script
# cur_dir = os.path.dirname(__file__)
CREDENTIALS_PATH = "/Users/nicoloruffini/projects/open-insider/utils/credentials/uplifted-name-410515-1b457124323f.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH


client = bigquery.Client()

# Perform a query.
q = """
    select * from `open_insider.trades_bronze` limit 10
"""
resp = client.query(q)  # API request
rows = resp.result()  # Waits for query to finish
print(type(rows))

temp = rows.to_dataframe()
print(type(temp))
print(temp)

# for row in rows:
#     vals = row.items()
#     print(type(vals))
#     print(vals)
#     break