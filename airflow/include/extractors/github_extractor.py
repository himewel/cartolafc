from datetime import datetime
import json
import logging

import requests

from . import AbstractExtractor


class GithubExtractor(AbstractExtractor):
    def __init__(self, path):
        self.base_url = (
            "https://api.github.com/repos/henriquepgomide/caRtola/contents/data"
        )
        self.path = path

    def extract(self, mode, **kwargs):
        execution_date = kwargs["execution_date"]

        if mode == "static":
            self.extract_static_files(execution_date)
        elif mode == "dynamic":
            self.extract_dynamic_files(execution_date)

    def normalize_filename(self, blob_name, year=""):
        normalized_name = blob_name.replace(year, "")
        normalized_name = normalized_name.replace(".csv", "")
        normalized_name = normalized_name.replace("-", "_")
        normalized_name = "_".join([x for x in normalized_name.split("_") if x])
        return normalized_name

    def extract_static_files(self, execution_date):
        response = requests.get(self.base_url)
        blob_list = json.loads(response.text)

        logging.info("Starting extraction...")
        for blob in blob_list:
            if blob["name"].endswith(".csv"):
                normalized_name = self.normalize_filename(blob["name"])

                folder = f"{self.path}/{execution_date.date()}/{normalized_name}"
                filename = "1.csv"
                self.save_file(blob["download_url"], folder, filename)
        logging.info("Success...")

    def extract_dynamic_files(self, execution_date):
        response = requests.get(f"{self.base_url}/{execution_date.year}")
        blob_list = json.loads(response.text)

        logging.info("Starting extraction...")
        for blob in blob_list:
            if blob["name"].endswith(".csv"):
                normalized_name = self.normalize_filename(
                    blob_name=blob["name"],
                    year=str(execution_date.year),
                )

                if normalized_name.startswith("rodada"):
                    suffix = normalized_name.split("_")[-1]
                    normalized_name = "rodada"
                else:
                    suffix = "1"

                folder = f"{self.path}/{execution_date.date()}/{normalized_name}"
                filename = f"{suffix}.csv"
                self.save_file(blob["download_url"], folder, filename)
        logging.info("Success...")
