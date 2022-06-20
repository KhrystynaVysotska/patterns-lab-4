EVENT_HUB_CONNECTION_STRING = "Endpoint=sb://lab-event-hub-namespace.servicebus.windows.net/;SharedAccessKeyName=spark-notebook-consumer;SharedAccessKey=uyblqDUhNJcHx5zsyifrozOx+y6k/d0QSaWrKyUul/Y=;EntityPath=event-hub-v4"
MAX_EVENTS_PER_TRIGGER = 1000

ELASTICSEARCH_USERNAME = "elastic"
ELASTICSEARCH_PASSWORD = "SqukDDwtIhltRKZWK38Bskqd"
ELASTICSEARCH_CLOUD_ID = "nosql-lab-elasticsearch:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDM5ZjBkNzNjNWY2MTQxNjdiNjYxZDVhODA2NWMzZTc5JGQxYmExOThiN2Q2YTRhMzg5OGI4ZDcwOWM5ZWRlZGNh"

VIOLATIONS_STATS_INDEX = "violations_stats_index"

import json
from datetime import datetime as dt

from pyspark.sql.functions import sum, avg, count
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, to_date, date_format

from elasticsearch import Elasticsearch

elastic_client = Elasticsearch(
    cloud_id=ELASTICSEARCH_CLOUD_ID,
    basic_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
)

configs = {}

startOffset = "-1"

startingEventPosition = {
    "offset": startOffset,
    "seqNo": -1,  # not in use
    "enqueuedTime": None,  # not in use
    "isInclusive": True
}

configs = {"eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
    EVENT_HUB_CONNECTION_STRING),
           "eventhubs.startingPosition": json.dumps(startingEventPosition),
           "eventhubs.maxEventsPerTrigger": MAX_EVENTS_PER_TRIGGER}

# STREAM DATA FROM EVENT HUB

df = spark.readStream.format("eventhubs").options(**configs).load()

# PREPARE DATA

# schema
schema_columns = ["plate", "state", "license_type", "summons_number", "issue_date", "violation_time", "violation",
                  "judgment_entry_date", "fine_amount", "penalty_amount", "interest_amount", "reduction_amount",
                  "payment_amount", "amount_due", "precinct", "county", "issuing_agency", "violation_status",
                  "summons_image"]
schema = StructType(fields=[StructField(column_name, StringType(), True) for column_name in schema_columns])

# body
df = df.withColumn("body", df["body"].cast("string"))
df = df.withColumn("body", from_json(col("body"), schema)).select("body.*")

# columns datatypes
df = df.withColumn("summons_number", df["summons_number"].cast('long'))
df = df.withColumn("issue_date", to_date(col("issue_date"), "MM/dd/yyyy"))
df = df.withColumn("judgment_entry_date", to_date(col("judgment_entry_date"), "MM/dd/yyyy"))
df = df.withColumn("fine_amount", df["fine_amount"].cast('integer'))
df = df.withColumn("penalty_amount", df["penalty_amount"].cast('integer'))
df = df.withColumn("interest_amount", df["interest_amount"].cast('integer'))
df = df.withColumn("reduction_amount", df["reduction_amount"].cast('integer'))
df = df.withColumn("payment_amount", df["payment_amount"].cast('integer'))
df = df.withColumn("amount_due", df["amount_due"].cast('integer'))

# CLEAN DATA

df = df.na.drop(subset=["issue_date", "state", "violation"])

# GROUP DATA

violation_stats = df.groupBy("issue_date", "state", "violation").agg(count("violation").alias("violations_number"),
                                                                     sum("payment_amount").alias("total_fine"))


# ELASTICSEARCH HELPER FUNCTIONS

def dataframe_to_actions(df):
    for record in df.rdd.map(lambda row: row.asDict()).collect():
        id = str(record["issue_date"]) + record["state"] + record["violation"]
        yield ('{ "index" : { "_index" : "%s", "_id" : "%s" }}' % (VIOLATIONS_STATS_INDEX, id))
        yield (json.dumps(record, default=str))


def send_to_elastic(df, *args):
    response = elastic_client.bulk(operations=dataframe_to_actions(df))
    print(f"Success: {not response['errors']}")


# SEND DATA TO ELASTICSEARCH

query = violation_stats \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(send_to_elastic) \
    .start()

query.awaitTermination()
