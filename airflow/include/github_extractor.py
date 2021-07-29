import json
import logging
import os

import fsspec
import requests
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook


class GithubExtractor:
    def __init__(self, base_url, path):
        self.base_url = base_url
        self.path = path

    def get_conn(self):
        conn_string = HDFSHook.get_connection("hdfs_default").get_uri()
        return conn_string

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

        # make the api request
        logging.debug(f"Downloading file from {download_url}...")
        csv_response = requests.get(download_url)
        csv_text = csv_response.text

        # create file if does not exist and overwrite if exists
        logging.debug(f"Creating file {filename} on hdfs...")
        hdfs = self.get_conn()
        logging.info(f"Saving file at {hdfs}/{filename}...")
        with fsspec.open(f"{hdfs}/{filename}", mode="w") as stream:
            stream.write(csv_text)

    def extract_static_files(self, year):
        response = requests.get(self.base_url)
        blob_list = json.loads(response.text)

        logging.info("Starting extraction...")
        for blob in blob_list:
            if blob["name"].endswith(".csv"):
                normalized_name = self.normalize_filename(blob["name"])

                folder = f"raw/{year}/{normalized_name}"
                filename = "1.csv"
                self.save_file(blob["download_url"], folder, filename)
        logging.info("Success..")

    def extract_dynamic_files(self, year):
        response = requests.get(f"{self.base_url}/{year}")
        blob_list = json.loads(response.text)

        logging.info("Starting extraction...")
        for blob in blob_list:
            if blob["name"].endswith(".csv"):
                normalized_name = self.normalize_filename(blob["name"], year)

                if normalized_name.startswith("rodada"):
                    suffix = normalized_name.split("_")[-1]
                    normalized_name = "rodada"
                else:
                    suffix = "1"

                folder = f"raw/{year}/{normalized_name}"
                filename = f"{suffix}.csv"
                self.save_file(blob["download_url"], folder, filename)
        logging.info("Success...")
