import json
import logging
import os

import requests


class RawExtractor:
    def __init__(self, base_url, path):
        self.base_url = base_url
        self.path = path

    def normalize_filename(self, blob_name, year=""):
        normalized_name = blob_name.replace(year, "")
        normalized_name = normalized_name.replace(".csv", "")
        normalized_name = normalized_name.replace("-", "_")
        normalized_name = "_".join([x for x in normalized_name.split("_") if x])
        return normalized_name

    def save_file(self, download_url, folder, filename):
        # create folder files
        logging.debug(f"Creating folder at {folder}...")
        filename = f"{folder}/{filename}"
        os.makedirs(folder, exist_ok=True)

        # make the api request
        logging.debug(f"Downloading file from {download_url}...")
        csv_response = requests.get(download_url)
        csv_text = csv_response.text

        # create file if does not exist and append if exists
        logging.debug(f"Downloading file {filename}...")
        with open(file=filename, mode="w+") as stream:
            stream.write(csv_text)

    def extract_static_files(self, year):
        response = requests.get(self.base_url)
        blob_list = json.loads(response.text)

        for blob in blob_list:
            if blob["name"].endswith(".csv"):
                normalized_name = self.normalize_filename(blob["name"])

                folder = f"{self.path}/{year}/{normalized_name}"
                filename = "1.csv"
                self.save_file(blob["download_url"], folder, filename)

    def extract_dynamic_files(self, year):
        response = requests.get(f"{self.base_url}/{year}")
        blob_list = json.loads(response.text)

        for blob in blob_list:
            if blob["name"].endswith(".csv"):
                normalized_name = self.normalize_filename(blob["name"], year)

                if normalized_name.startswith("rodada"):
                    suffix = normalized_name.split("_")[-1]
                    normalized_name = "rodada"
                else:
                    suffix = "1"

                folder = f"{self.path}/{year}/{normalized_name}"
                filename = f"{suffix}.csv"
                self.save_file(blob["download_url"], folder, filename)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    extractor = RawExtractor(
        base_url="https://api.github.com/repos/henriquepgomide/caRtola/contents/data",
        path=f"./data/raw",
    )

    extractor.extract_static_files("2020")
    extractor.extract_dynamic_files("2020")
