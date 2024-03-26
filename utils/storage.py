from google.cloud import storage
import os
from datetime import datetime
from .credentials import config

class CloudStorage():
    """ Class used to hand logging to GCS. """
    def __init__(self, name) -> None:
        
        cur_dir = os.path.dirname(__file__)
        CREDENTIALS_PATH = f"{cur_dir}/credentials/uplifted-name-410515-1b457124323f.json"
        BUCKET_NAME = config.BUCKET_NAME

        # Setting up environ variables with json service principal file
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

        # Get current datetime object
        current_datetime = datetime.now()
        date_str = current_datetime.strftime("%Y-%m-%d")

        # Format datetime object as a string
        timestamp_string = current_datetime.strftime("%Y-%m-%d %H:%M")

        storage_client = storage.Client()
        my_bucket = storage_client.get_bucket(BUCKET_NAME)
        self.blob = my_bucket.blob(f"open_insider/logs/log_{date_str}/{timestamp_string}_{name}.txt")
        self.f = self.blob.open("w")

    def print(self, string):
        self.f.write(string)
        print(string)

    def close_file(self):
        """ Close file object - will not create file in GCS without calling this. """
        self.f.close()