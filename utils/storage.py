from google.cloud import storage
import os
from datetime import datetime

class CloudStorage():
    def __init__(self, name) -> None:
        # Setting up environ variables with json service principal file
        cur_dir = os.path.dirname(__file__)
        CREDENTIALS_PATH = f"{cur_dir}/credentials/uplifted-name-410515-1b457124323f.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

        # Get current datetime object
        current_datetime = datetime.now()
        date_str = current_datetime.strftime("%Y-%m-%d")

        # Format datetime object as a string
        timestamp_string = current_datetime.strftime("%Y-%m-%d %H:%M")


        storage_client = storage.Client()
        my_bucket = storage_client.get_bucket("nicolo-bucket")
        self.blob = my_bucket.blob(f"open_insider/logs/log_{date_str}/{timestamp_string}_{name}.txt")

        self.f = self.blob.open("w")

    def print(self, string):
        self.f.write(string)
        print(string)

    def close_file(self):
        self.f.close()


if __name__ == "__main__":
    print("this is my storage file")

    sto = CloudStorage("TESTING")
    sto.print("trying to type this")
    sto.close_file()