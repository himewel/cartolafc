import json
import os

import requests


class Extractor:
    def __init__(self, base_url, path):
        self.base_url = base_url
        self.path = path

    def extract(self, year):
        response = requests.get(f"{self.base_url}/{year}")
        blob_list = json.loads(response.text)

        os.makedirs(f"{self.path}/{year}", exist_ok=True)

        for blob in blob_list:
            if blob["name"].endswith(".csv"):
                csv_response = requests.get(blob["download_url"])
                csv_text = csv_response.text
                with open(f"{self.path}/{year}/{blob['name']}") as stream:
                    stream.write(csv_text)
