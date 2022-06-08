from typing import List

from extractor import Extractor
from loader import LoadStrategy


class ETL:
    def __init__(self, load_strategy: LoadStrategy, datasource_url: str):
        self.__load_strategy = load_strategy
        self.__datasource_url = datasource_url

    def load(self, data: List[str]) -> None:
        self.__load_strategy.load(data)

    def extract(self):
        return Extractor.extract_from(self.__datasource_url, chunk_size=200)

    def start(self):
        try:
            for data in self.extract():
                self.load(data)
                yield {"msg": f"Loaded data chunk: {len(data)}", "category": "info"}
            yield {"msg": "Finished extracting and loading successfully", "category": "success"}
        except Exception as e:
            yield {"msg": f"Error in etl: {e}", "category": "error"}

