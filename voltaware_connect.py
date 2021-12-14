#!/usr/bin/env python3
from config import read_config
import typing as tp
import threading
import logging
import json
import time
import pika

import robonomicsinterface as RI


def callback(ch, method, properties, body) -> None:
    """
    get data from sensors and save it to dictionary
    """

    data = body.decode("utf-8")
    data_js = json.loads(data)
    data_js["event"].pop("timestampDeltaSecs", None)
    data_js["event"].pop("transients", None)
    data_js["event"].pop("harmonics", None)
    Account.data_id[data_js["sensor"]["id"]] = str(data_js["event"])
    ch.basic_ack(delivery_tag=method.delivery_tag)


class Account:

    data_id: tp.Dict[int, str] = {}

    def __init__(self, name: str, config: dict) -> None:
        logging.info(f"creating account instance with name {name}")
        self.name = name
        # self.pub_adr = config["accounts"][self.name]["pub_adr"]
        self.sensor_id = int(config["accounts"][self.name]["id"])
        self.seed = config["accounts"][self.name]["seed"]
        self.url = config["substrate_wss"]
        self.substrate = RI.RobonomicsInterface(seed=self.seed, remote_ws=self.url)
        logging.info(f"{self.name} successfully created")

    def send_data(self) -> None:
        """
        send data to frontier parachain "robonomics" from all devices
        """
        logging.info(f"send data from account: {self.name}")
        if self.sensor_id in Account.data_id:
            account_thread = threading.Thread(
                target=self.substrate.record_datalog, args=(Account.data_id[self.sensor_id],)
            )
            account_thread.start()
        else:
            logging.warning(f"There is not sensor with id: {self.sensor_id}")


def main() -> None:
    # set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    # read config file
    config = read_config()

    # Connect to RabbitMQ server
    logging.info("establishing connection to RabbitMQ")
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
        logging.info("Successfully established connection to RabbitMQ")
    except Exception as e:
        channel.join()
        connection.close()
        logging.error(f"Failed to connect to RabbitMQ: {e}")
    time.sleep(10)

    # get all accounts names
    accounts_names_list = list(config["accounts"])
    # create Account's inctance for every account in config
    accounts_list: tp.List[Account] = []
    for x in range(len(accounts_names_list)):
        accounts_list.append(Account(accounts_names_list[x], config))

    while True:
        try:
            threads_num = threading.active_count()
            if threads_num > 90:
                logging.warning("Too many opened sending requests, waiting")
                time.sleep(12)
            for account in accounts_list:
                account.send_data()
            time.sleep(10)
        except KeyboardInterrupt:
            channel.join(timeout=5)
            connection.close()
            break


if __name__ == "__main__":
    main()
