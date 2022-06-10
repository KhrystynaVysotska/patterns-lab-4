import json
from abc import ABC, abstractmethod
from typing import List

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

from etl.constants import CONNECTION_STRING, EVENTHUB_NAME
from helpers import get_or_create_eventloop


class LoadStrategy(ABC):
    @abstractmethod
    def load(self, data: List[str]):
        pass


class ConsoleLoadStrategy(LoadStrategy):
    def load(self, data: List[str]):
        print(data)


class FileLoadStrategy(LoadStrategy):
    def __init__(self, filename: str):
        self.filename = f"extracted/{filename}"

    def load(self, data: List[str]):
        with open(self.filename, "a") as file:
            for record in data:
                file.write("%s\n" % record)


class EventHubLoadStrategy(LoadStrategy):
    def __init__(self):
        self.producer = None
        self.configs = {"conn_str": CONNECTION_STRING, "eventhub_name": EVENTHUB_NAME}

    def get_producer(self):
        if not self.producer:
            self.producer = EventHubProducerClient.from_connection_string(**self.configs)
        return self.producer

    async def load_async(self, data):
        async with self.get_producer():
            batch = await self.producer.create_batch()

            for record in data:
                batch.add(EventData(json.dumps(record)))

            await self.producer.send_batch(batch)

    def load(self, data: List[str]):
        loop = get_or_create_eventloop()
        loop.run_until_complete(self.load_async(data))
