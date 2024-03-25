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


class Database():
    def __init__(self, collection) -> None:
        cur_dir = os.path.dirname(__file__)
        CREDENTIALS_PATH = f"{cur_dir}/credentials/uplifted-name-410515-1b457124323f.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
        self.db = firestore_v1.Client(database="openinsider")

        self.collection = collection

    def get_all_docs(self):
        coll_ref = db.collection(self.collection)

        for doc in coll_ref:
            doc_data = doc.to_dict()
            print(doc_data)


    def add_document(self, doc):
        doc_ref = self.db.collection(self.collection).document()
        doc_ref.set(doc)


    

db = Database("openinsider")

for item in data:
    db.add_document(item)



