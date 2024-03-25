import os
import pandas as pd
from google.cloud import bigquery
from alpaca.trading.models import Order
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from datetime import date, datetime



class Database:
    def __init__(self) -> None:
        # Setting up environ variables with json service principal file
        cur_dir = os.path.dirname(__file__)
        CREDENTIALS_PATH = f"{cur_dir}/credentials/uplifted-name-410515-1b457124323f.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
        self.client = bigquery.Client()


    def get_client(self):
        return self.client

    def drop_table(self, table):
        try:
            self.client.delete_table(f"open_insider.{table}", not_found_ok=True)
        except Exception as e:
            raise Exception("Error with drop_table:", e)
        
    def get_num_rows(self, table):
        try:
            table = self.client.get_table(f"open_insider.{table}")
            return table.num_rows
        except Exception as e:
            return 0

    
    def query(self, query):
        """
        Send a query to BigQuery. Returns a iterator object - view below link to see methods on the results
          https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.table.RowIterator
        """
        resp = self.client.query(query)  # API request
        rows = resp.result()  # Waits for query to finish
        return rows
    
    def get_table_data(self, table):
        full_table = f"open_insider.{table}"
        try:
            rows = self.client.query(f"select * from `{full_table}`")
            data = []
            for row in rows:
                data.append(row.values())
            return data
        except:
            print(f"{full_table} doesn't exists")
            return []

        
    def query_to_pd(self, query):
        resp = self.client.query(query)
        resp = resp.result()
        df = resp.to_dataframe()
        return df
    
    def clean_trade(self, trade):
        temp_trade = trade

        temp_trade["price"] = trade["price"].replace("$", "").replace(",", "")
        temp_trade["qty"] = trade["qty"].replace(",", "")
        temp_trade["owned"] = trade["owned"].replace(",", "")
        temp_trade["value"] = trade["value"].replace("$", "").replace(",", "")

        return temp_trade
    
    def clean_trades(self, trades):
        lst = []
        for t in trades:
            cleaned = self.clean_trade(t)
            lst.append(cleaned)

        return lst


    def insert_trades(self, table, trades):
        """
        Inserts trades into  table:

        Args:
            table: table you want to insert into. Must exist before
            trades: list of trades in json format
        """
        if trades:
            resp = self.client.insert_rows_json(f"uplifted-name-410515.open_insider.{table}", trades)
            if resp:
                print("INSERT FAILED:", resp)

        print(f"Added {len(trades)} records to {table}")
        

    # Store new trades wherever - need to make sure self.db is defined correctly
    def cache_trades(self, trades, full_load=False):
        """
        Inserting new trades.

        Args:
            trades is a list of dictionaries

        Assumptions:
            assumes all trades are in the same month-year.
        """
        # Not a full load so we need to check against existing records
        if full_load == False:
            print("Not a full load - checking against existing records")

            # Getting year of current trades so we only have to check against that year 
            year_of_trades = int(trades[0]["filing_date"].split("-")[0])
            month_of_trades = int(trades[0]["filing_date"].split("-")[1])

            # Getting existing pks to check against if not a full load
            query = f"""
            SELECT filing_date, ticker, insider_name, trade_type 
            FROM `open_insider.trades_bronze` 
            WHERE 
                EXTRACT(YEAR FROM filing_date) = {year_of_trades} AND
                EXTRACT(MONTH FROM filing_date) = {month_of_trades}
            """
            existing_pks = self.query(query)

            # Converting to list of tuples
            existing_pks_list = [(row["filing_date"].strftime("%Y-%m-%d %H:%M:%S"), row["ticker"], row["insider_name"], row["trade_type"]) for row in existing_pks]

            # Insert data into the table if it doesn't already exists
            lst_to_insert = []
            for trade in trades:
                # Getting pk of trade
                temp_pk = (trade["filing_date"], trade["ticker"], trade["insider_name"], trade["trade_type"])

                # Only inserting into table if it doesn't already exists
                if temp_pk not in existing_pks_list:
                    trade = self.clean_trade(trade)
                    lst_to_insert.append(trade)
            
            self.insert_trades("trades_bronze", lst_to_insert)

        else:
            print("full load - not checking against existing records")
            lst_to_insert = []
            for trade in trades:
                trade = self.clean_trade(trade)
                lst_to_insert.append(trade)

            self.insert_trades("trades_bronze", lst_to_insert)

        return lst_to_insert
    

    def create_table_from_trades(self, table, trades):
        """
        Creates new table from json. This WILL append to table if it already exists
        """
        # Configuring schema
        schema = [
            bigquery.SchemaField("x", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("filing_date", "TIMESTAMP", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("trade_date", "DATE", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("ticker", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("company_name", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("insider_name", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("title", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("trade_type", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("price", "FLOAT", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("qty", "INTEGER", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("owned", "INTEGER", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("change_of_owned", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("value", "INTEGER", "NULLABLE", None, None, (), None),
        ]
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = schema


        cleaned_trades = self.clean_trades(trades)
        resp = self.client.load_table_from_json(json_rows=cleaned_trades, destination=f"open_insider.{table}", job_config=job_config)
        resp.result() # Wait for it to finish
        return resp



    def cache_new_stock(self, stock_dic, table="ticker_data"):
        """
        Caching stock data.

        Params:
            stock_dic is a dictionary.
            {'EPD': [{
                'close': 26.44,
                'high': 26.525,
                'low': 25.35,
                'open': 25.69,
                'symbol': 'EPD',
                'timestamp': datetime.datetime(2016, 1, 4, 5, 0, tzinfo=TzInfo(UTC)),
                'trade_count': 49721.0,
                'volume': 11726088.0,
                'vwap': 26.116505}], 
            'LAYN': [{
                'close': 5.27,
                'high': 5.39,
                'low': 5.13,
                'open': 5.25,
                'symbol': 'LAYN',
                'timestamp': datetime.datetime(2016, 1, 4, 5, 0, tzinfo=TzInfo(UTC)),
                'trade_count': 1539.0,
                'volume': 449188.0,
                'vwap': 5.255559}],
            ....,
            }
        """
        lst_to_insert = []
        for ticker in stock_dic:
            data = stock_dic[ticker][0]
            temp_dic = {
                "ticker": ticker, 
                "date": data.timestamp.strftime('%Y-%m-%d'), 
                "open": data.open, 
                "high": data.high, 
                "low": data.low, 
                "close": data.close, 
                "trade_count": data.trade_count, 
                "volume": data.volume, 
                "vwap": data.vwap
            }
            lst_to_insert.append(temp_dic)

        if table == "recent_ticker_data":
            self.client.load_table_from_json(json_rows=lst_to_insert, destination=f"open_insider.{table}")
        else:

            try:
                resp = self.client.insert_rows_json(f"uplifted-name-410515.open_insider.{table}", lst_to_insert)
                if resp:
                    print("RESPONSE", resp)
            except Exception as e:
                raise Exception(f"FAILED: {e}")
            

    def insert_order(self, id, ticker, trade_type, order_type, amount):
        """
        Inserting order into my_orders table. 'Order' class - https://github.com/alpacahq/alpaca-py/blob/881b5deddef54682f5d63ace8a6c9bff6c49868d/alpaca/trading/models.py
        Args:
            ticker str: symbol of stock
            trade_type str: either purchase or sale
            order_type str: indicating whole share or fractional "D" - dollar amount "S" - share amount
            amount float: either dollar amount or share amount
        """
        # Want to store
            # date
            # ticker
            # price of ticker
            # num_shares
            # value
        
        # Get the current timestamp
        current_timestamp = datetime.now().timestamp()
        current_datetime = datetime.fromtimestamp(current_timestamp)
        today = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

        data = {            
            "amount": float(round(amount, 2)),
            "order_type": order_type,
            "trade_type": trade_type,
            "ticker": ticker,
            "placed_date": today,
            "alpaca_order_id": str(id)
        }
        lst = [data]
        resp = self.client.load_table_from_json(json_rows=lst, destination=f"open_insider.my_orders")
        return resp


        

