from typing import List

from etl.extractor import Extractor
from etl.loader import LoadStrategy
from redis_api.api import RedisApi
from redis_api.constants import COMPLETED, IN_PROCESS, FAILED


class ETL:
    def __init__(self, load_strategy: LoadStrategy, datasource_url: str):
        self.__load_strategy = load_strategy
        self.__datasource_url = datasource_url
        self.__redis_api = RedisApi()

    def set_load_strategy(self, load_strategy: LoadStrategy):
        self.__load_strategy = load_strategy

    def load(self, data: List[str]) -> None:
        self.__load_strategy.load(data)

    def extract(self, initial_offset):
        return Extractor.extract_from(self.__datasource_url, initial_offset)

    def __start(self, initial_offset=0):
        self.__redis_api.set_extract_info(self.__datasource_url, status=IN_PROCESS, records=initial_offset)
        try:
            for data in self.extract(initial_offset):
                self.load(data)
                total_records = self.__redis_api.increment_records(self.__datasource_url, len(data))
                yield {"msg": f"Loaded data chunk: {len(data)}. Total: {total_records}", "level": "message"}
            self.__redis_api.set_status(self.__datasource_url, COMPLETED)
            yield {"msg": "Finished extracting and loading successfully", "level": "success"}
        except Exception as e:
            self.__redis_api.set_status(self.__datasource_url, FAILED)
            yield {"msg": f"Error in etl: {e}", "level": "error"}

    def start(self):
        if not self.__redis_api.exists(self.__datasource_url):
            yield from self.__start()
        else:
            timestamp = self.__redis_api.get_timestamp(self.__datasource_url)
            status = self.__redis_api.get_status(self.__datasource_url)

            if status == COMPLETED:
                self.__redis_api.set_message(self.__datasource_url, "Attempt to reprocess data")
                yield {"msg": f"Data from {self.__datasource_url} is already processed at {timestamp}", "level": "info"}
            else:
                records = self.__redis_api.get_records(self.__datasource_url)
                yield {"msg": f"Latest info: status: {status}, records: {records}, time: {timestamp}", "level": "info"}
                yield from self.__start(initial_offset=records)
