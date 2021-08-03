import logging
from abc import ABC, abstractmethod

import fsspec
import requests
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook


class AbstractExtractor(ABC):
    def __init__(self, base_url, path):
        self.base_url = base_url
        self.path = path

    def get_conn(self):
        conn_string = HDFSHook.get_connection("hdfs_default").get_uri()
        return conn_string

    def save_file(self, download_url, folder, filename):
        # create folder files
        logging.debug(f"Creating folder at {folder}...")
        filename = f"{folder}/{filename}"

        # make the api request
        logging.debug(f"Downloading file from {download_url}...")
        response = requests.get(download_url)
        response.raise_for_status()
        text = response.text

        # create file if does not exist and overwrite if exists
        logging.debug(f"Creating file {filename} on hdfs...")
        hdfs = self.get_conn()
        logging.info(f"Saving file at {hdfs}/{filename}...")
        with fsspec.open(f"{hdfs}/{filename}", mode="w") as stream:
            stream.write(text)

    @abstractmethod
    def extract(self):
        pass
