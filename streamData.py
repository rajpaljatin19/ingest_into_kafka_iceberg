import json, pyarrow.parquet as pq, pyarrow.compute as pc, pandas as pd, pyarrow as pa, datetime, subprocess, sys, os, random, time, logging, subprocess, os, traceback, math, base64, random, string, psutil, uuid, requests, socket, argparse, os.path, re, json, datetime
from pyarrow import csv
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyarrow import fs
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime, timezone
from time import gmtime, strftime
from math import isnan
from subprocess import PIPE, Popen
from kafka import KafkaProducer
from kafka.errors import KafkaError
from time import sleep

response_list = []

apikey = '**********'

stocks = ["ORCL", "GOOG", "AMZN",  "NFLX", "TSLA", "GOOGL"]

base_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='
remaining_url = '&interval=5min&apikey='

def call_api(stock):
    url = base_url + stock + remaining_url + apikey
    response = requests.get(url)
    return response.json() if response.ok else None

responses = []
for stock in stocks:
    response = call_api(stock)
    if response:
        responses.append(response)

producer = KafkaProducer(key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode('ascii'),bootstrap_servers='Jatins-MacBook-Air.local:9092',retries=3)

tablename = "***"
schemaname = "*****" 
s3location = "s3://pyiceberg"
local_data_dir = "/Users/jatinrajpal/workspace/iceberg"


warehouse_path = "/tmp/warehouse"
catalog = SqlCatalog(
    "docs",
    **{
        "uri": f"sqlite:////tmp/warehouse/pyiceberg_catalog.db",
        "warehouse": "******",
        "s3.endpoint": "******",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "*****",
        "s3.secret-access-key": "******",
    },
)

for element in responses:
    ts = time.time()
    uuid_key = '{0}_{1}'.format(strftime("%Y%m%d%H%M%S",gmtime()),uuid.uuid4())
    try:
        producer.send('test_topic', key=uuid_key, value=element)
        producer.flush()
    except:
        print("Error sending messages to Kafka")


df = pa.Table.from_pylist(responses)

table = None
try:
  table = catalog.create_table(
  f'{schemaname}.{tablename}',
  schema=df.schema,
  location=s3location,
  )
except:
  print("Table exists, append " + tablename)    
  table = catalog.load_table(f'{schemaname}.{tablename}')

  table.append(df)
  responses = []
  df = None 

  time.sleep(0.05)
producer.close()
