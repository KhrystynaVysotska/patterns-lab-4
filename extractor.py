import requests


class Extractor:
    @staticmethod
    def extract_from(dataset_url, chunk_size):
        offset = 0
        limit = chunk_size

        can_extract = True
        while can_extract:
            data = requests.get(dataset_url, params={"$limit": limit, "$offset": offset}).json()
            if data:
                yield data
                offset += limit
            else:
                can_extract = False
