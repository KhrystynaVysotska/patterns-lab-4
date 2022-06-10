import redis

from datetime import datetime

from redis_api.constants import STATUS_KEY, RECORDS_KEY, START_TIMESTAMP, HOSTNAME, PORT, PASSWORD


class RedisApi:
    def __init__(self):
        self.redis = redis.StrictRedis(host=HOSTNAME, port=PORT, password=PASSWORD, ssl=True)

    def exists(self, url: str) -> int:
        return self.redis.exists(url)

    def get_status(self, url: str) -> str:
        return self.redis.hget(name=url, key=STATUS_KEY).decode("utf-8")

    def get_records(self, url: str) -> int:
        return int(self.redis.hget(name=url, key=RECORDS_KEY))

    def get_timestamp(self, url: str) -> str:
        return self.redis.hget(name=url, key=START_TIMESTAMP).decode("utf-8")

    def set_status(self, url, status: str) -> int:
        return self.redis.hset(name=url, key=STATUS_KEY, value=status)

    def increment_records(self, url, records_amount) -> int:
        return self.redis.hincrby(url, RECORDS_KEY, records_amount)

    def set_message(self, url: str, message: str) -> int:
        timestamp = str(datetime.now())
        return self.redis.hset(name=url, key=timestamp, value=message)

    def set_extract_info(self, url, status, records) -> bool:
        timestamp = str(datetime.now())
        mapping = {START_TIMESTAMP: timestamp, STATUS_KEY: status, RECORDS_KEY: records}
        return self.redis.hmset(name=url, mapping=mapping)
