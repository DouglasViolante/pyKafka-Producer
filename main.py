import datetime
import json
import logging
import random
import sys
import uuid

from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep

from confluent_kafka import Producer
import socket

def acked(err, msg):
    if err is not None:
        logging.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        logging.info("Message produced: %s" % (str(msg)))

def write_to_kafka(producer, data):

    producer.produce(topic="transactions", value=bytes(json.dumps(data), encoding='utf-8'), callback=acked)
    producer.poll(1)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    conf = {'bootstrap.servers': "host.docker.internal:9094",
            'client.id': socket.gethostname()}

    producer = Producer(conf)
    
    sched = BackgroundScheduler()    

    def produceDummyData():
        newDummyData = [(
            str(uuid.uuid4().hex),
            round(random.uniform(0.1, 9999.9), 2),
            random.choice(["Debit", "Credit"]),
            datetime.datetime.now().ctime(),
            random.choice([True, False])
        )]
        logging.info("start writing data to kafka")
        write_to_kafka(producer, data=newDummyData)

    sched.add_job(produceDummyData, 'interval', seconds=5)

    sched.start()

    while True:
        sleep(1)

    