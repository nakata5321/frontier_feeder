#
# this script will save all data from RabbitMQ
#
import threading
import time
import json
import pika
from ..config import read_config


def callback(ch, method, properties, body) -> None:
    """
    get data from sensors and save it to dictionary
    """
    try:
        file = open("data.txt", "a", encoding="utf-8")
        data = body.decode("utf-8")
        data_js = json.loads(data)
        file.write(str(data_js))
    finally:
        file.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)


# read config file
config = read_config()

# Connect to RabbitMQ server
credential = pika.PlainCredentials(
    config["RabbitMQ"]["credentials"]["login"],
    config["RabbitMQ"]["credentials"]["passwrd"],
)
params = pika.ConnectionParameters(
    host=config["RabbitMQ"]["host"],
    port=int(config["RabbitMQ"]["port"]),
    virtual_host="/",
    heartbeat=60,
    credentials=credential,
)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.basic_qos(prefetch_count=10)
channel.basic_consume("aira-event", callback)
try:
    channel = threading.Thread(target=channel.start_consuming)
    channel.start()
except Exception as e:
    channel.join()
    connection.close()
time.sleep(2)
