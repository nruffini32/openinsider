data = [
    {
        "x": "",
        "filing_date": "2024-03-21 12:28:34",
        "trade_date": "2024-03-15",
        "ticker": "ISDR",
        "company_name": "Issuer Direct Corp",
        "insider_name": "Topline Capital Partners, LP",
        "title": "10%",
        "trade_type": "P - Purchase",
        "price": "$11.90",
        "qty": "+10,498",
        "owned": "556,051",
        "change_of_owned": "+2%",
        "value": "+$124,926",
    },
    {
        "x": "",
        "filing_date": "2024-03-21 12:17:50",
        "trade_date": "2024-03-18",
        "ticker": "CTGO",
        "company_name": "Contango Ore, Inc.",
        "insider_name": "Compofelice Joseph S",
        "title": "Dir",
        "trade_type": "S - Sale",
        "price": "$22.00",
        "qty": "-5,826",
        "owned": "176,878",
        "change_of_owned": "-3%",
        "value": "-$128,172",
    },
    {
        "x": "M",
        "filing_date": "2024-03-21 11:56:58",
        "trade_date": "2024-03-19",
        "ticker": "ANDE",
        "company_name": "Andersons, Inc.",
        "insider_name": "Bowe Patrick E.",
        "title": "Pres, CEO",
        "trade_type": "S - Sale",
        "price": "$55.33",
        "qty": "-2,107",
        "owned": "88,546",
        "change_of_owned": "-2%",
        "value": "-$116,576",
    },
    {
        "x": "",
        "filing_date": "2024-03-21 11:28:42",
        "trade_date": "2024-03-20",
        "ticker": "GPC",
        "company_name": "Genuine Parts Co",
        "insider_name": "Needham Wendy B",
        "title": "Dir",
        "trade_type": "S - Sale",
        "price": "$154.92",
        "qty": "-3,250",
        "owned": "14,397",
        "change_of_owned": "-18%",
        "value": "-$503,490",
    },
    {
        "x": "",
        "filing_date": "2024-03-21 11:27:29",
        "trade_date": "2024-03-19",
        "ticker": "NUE",
        "company_name": "Nucor Corp",
        "insider_name": "Behr Allen C",
        "title": "EVP",
        "trade_type": "S - Sale",
        "price": "$190.19",
        "qty": "-10,000",
        "owned": "80,902",
        "change_of_owned": "-11%",
        "value": "-$1,901,866",
    },
]

from google.cloud import firestore_v1
import os
from google.cloud.firestore_v1.base_query import FieldFilter,Or


class FireStore():
    def __init__(self) -> None:
        cur_dir = os.path.dirname(__file__)
        CREDENTIALS_PATH = f"{cur_dir}/credentials/uplifted-name-410515-1b457124323f.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

        self.db = firestore_v1.Client(database="openinsider")
        self.col_ref = self.db.collection("all_trades")


    def get_all_trades(self):
        docs = (
            self.db.collection("all_trades")
            .stream()
        )

        # Iterate over the documents and store their IDs and data in a list
        documents_list = []

        for doc in docs:
            doc_data = doc.to_dict()
            doc_data['id'] = doc.id
            doc_data['docData'] = doc._data

            #print(doc._data)
            documents_list.append(doc_data)

        return documents_list


    def add_trade(self, trade):
        doc_ref = self.db.collection("all_trades").document()
        doc_ref.set(trade)
        print(doc_ref.id)


    def add_new_trades(self, trades, full_load=True):
        """
        Inserting new trades.

        Args:
            trades is a list of dictionaries

        Assumptions:
            assumes all trades are in the same year
        """
        cnt = 0
        # Not a full load so we need to check against existing records
        if full_load == False:
            print("Not a full load - checking against existing records")

            # Getting year of current trades so we only have to check against that year 
            year_of_trades = trades[0]["filing_date"].split("-")[0]

            # Getting existing pks to check against if not a full load
            query = f"""
            SELECT filing_date, ticker, insider_name, trade_type 
            FROM `open_insider.trades_bronze` 
            WHERE EXTRACT(YEAR FROM filing_date) = {year_of_trades}
            """
            existing_pks = self.query(query)

            query = self.col_ref.where(filter=FieldFilter("status", "IN", year_of_trades))

            #stream for results
            docs = query.stream()


            # Insert data into the table if it doesn't already exists
            for trade in trades:
                # Getting pk of trade
                temp_pk = (trade["filing_date"], trade["ticker"], trade["insider_name"], trade["trade_type"])

                # Only inserting into table if it doesn't already exists
                if temp_pk not in existing_pks:
                    self.add_trade(trade)
                    # Incrementing cnt to track records added
                    cnt += 1
                    if cnt == 5:
                        break
            

            #### HEREHHERHEHR TESTING INSERTIN
                
        else:
            print("full load - not checking against existing records")
            for trade in trades:
                self.insert_record(trade=trade)
                # Incrementing cnt to track records added
                cnt += 1


    

db = FireStore()
existing_trades = db.get_all_trades()
print(existing_trades)

# for item in data:
#     db.add_trade(item)



