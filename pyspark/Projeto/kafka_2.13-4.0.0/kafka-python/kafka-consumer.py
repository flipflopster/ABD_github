"""
This simple code illustrates a Kafka producer:
- read data from a topic in a Kafka messaging system.
- print out the data

As it stands, it should work with any data as long as the data is in JSON 
(one can modify the code to handle other types of data)

We use the Python client library kafka-python from 
    https://kafka-python.readthedocs.io/en/master/.

Also, see https://pypi.org/project/kafka-python/
"""
import os, json
from kafka import KafkaConsumer
from pyspark.sql import Row

#=======================================================
topic = 'crime-chicago'
# consumer_group = 'my-group'

#=======================================================

# Declare the topic
kafka_consumer = KafkaConsumer(
    bootstrap_servers = 'localhost:9092',
    api_version = (3, 9),
    auto_offset_reset = 'earliest',
    enable_auto_commit = False,
    # group_id = consumer_group,
)

kafka_consumer.subscribe([topic])

print("Starting to listen for messages on topic : " + topic + ". ")


for msg in kafka_consumer:
    # msg value and key may be raw bytes - decode if necessary!
    print ("Received message is [%s:%d:%d] key=%s value=%s" % (msg.topic, msg.partition,
            msg.offset, msg.key, msg.value.decode('utf-8')))
    
   
    
    
    # Note: msg can be stored in a database or subject to other processing
    # Also, we could have used other formats.

