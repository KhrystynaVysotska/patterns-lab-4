import requests

from etl.constants import CHUNK_SIZE


class Extractor:
    @staticmethod
    def extract_from(dataset_url, initial_offset):
        offset = initial_offset

        can_extract = True
        while can_extract:
            data = requests.get(dataset_url, params={"$limit": CHUNK_SIZE, "$offset": offset}).json()
            if data:
                yield data
                offset += CHUNK_SIZE
            else:
                can_extract = False
