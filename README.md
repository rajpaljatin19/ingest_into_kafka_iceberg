# ingest_into_kafka_iceberg

### Generate Alpha Vantage access key ###

Create key via [Alpha Vantage website](https://www.alphavantage.co/support/#api-key)

### MINIO ###

Deploy minio locally then start it via minio command line. More information can be taken from the [Minio documentation](https://min.io/docs/minio/macos/operations/install-deploy-manage/deploy-minio-single-node-single-drive.html#id6)

`jatinrajpal@Jatins-MacBook-Air minio % minio server /Users/jatinrajpal/workspace/minio`

(Above command will have an output like this)

`WebUI: http://192.168.1.156:50423 http://127.0.0.1:50423` 

   `RootUser: minioadmin`
   
   `RootPass: minioadmin`
   

### Start Kafka ###

Deploy Kafka and Zookeeper beforehand. More information can be taken from Kafka [documentation](https://kafka.apache.org/quickstart)

   `brew services start kafka`
   
   `brew services start zookeeper`
   
   `kafka-console-consumer --bootstrap-server <kafka_broker_address> --topic <topic_name> --from-beginning`

### create a local python virtual environment ##

   `python -m venv /path/to/new/virtual/environment`

### Starting Python virtual environment ##

 `source /path/to/new/virtual/environment/venv/bin/activate`

Then deploy all the required python packages which have been used in the scripts

### Pyiceberg setup ##

Setting up iceberg locally using it's [documentation](https://py.iceberg.apache.org/?source=post_page-----5d642e1170ae--------------------------------#connecting-to-a-catalog)

   `mkdir /tmp/warehouse`
   
   `python3 setupIceCatalogue.py`
   
   `python3 createNS.py`
   
   `find /tmp/warehouse/`

### Create namespace ### 

First create the namespace on Minio S3
   
   `python3 createNS.py`

### Run script to write to Kafka and further write to iceberg table ##

   `python3 writeData.py`
