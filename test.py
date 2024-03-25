from utils import InsiderScraper, Database
from google.cloud import bigquery


# Init instances
db = Database()
scraper = InsiderScraper()

# Get client
client = db.get_client()

# Get test data
data = scraper.get_recent_trades(10)
cleaned_data = db.clean_trades(data)

table = client.get_table("open_insider.trades_bronze")

print(table.schema)
# job_config = bigquery.LoadJobConfig()
# job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
# job_config.schema = table.schema
# resp = client.load_table_from_json(json_rows=cleaned_data, destination="open_insider.testing_table", job_config=job_config)


# resp = db.create_table_from_trades("staging_trades", data)
# print(resp)
