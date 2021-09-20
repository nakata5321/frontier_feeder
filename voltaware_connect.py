#!/usr/bin/env python3
import typing as tp
import threading
import logging
import json
import time
import pika
import yaml

from substrateinterface import SubstrateInterface, Keypair


def read_config(path: str) -> tp.Dict[str, str]:
    try:
        with open(path) as stream:
            logging.info("start read config")
            config = yaml.load(stream, Loader=yaml.FullLoader)
            logging.debug(f"Configuration dict: {stream}")
            return config
    except Exception as e:
        while True:
            logging.error(f"Configuration file is broken or not readable! {e}")
            time.sleep(5)


def callback(ch, method, properties, body) -> None:
    """
    get data from sensors and save it to dictionary
    """

    data = body.decode("utf-8")
    data_js = json.loads(data)
    data_js['event'].pop('timestampDeltaSecs', None)
    data_js['event'].pop('transients', None)
    data_js['event'].pop('harmonics', None)
    Account.data_id[data_js["sensor"]["id"]] = str(data_js['event'])
    ch.basic_ack(delivery_tag=method.delivery_tag)


class Account:

    data_id = {}

    def __init__(self, name: str, config: dict) -> None:
        logging.info(f"creating account instance with name {name}")
        self.name = name
        self.pub_adr = config['accounts'][self.name]['pub_adr']
        self.sensor_id = int(config["accounts"][self.name]["id"])
        self.seed = config["accounts"][self.name]["seed"]
        self.url = config["substrate_wss"]
        self.substrate = self.__substrate_connection()
        logging.info(f"{self.name} successfully created")

    def __substrate_connection(self) -> tp.Any:
        """
        establish connection to a specified substrate node
        """
        try:
            logging.info("Establishing connection to substrate node")
            substrate = SubstrateInterface(
                url=self.url,
                ss58_format=32,
                type_registry_preset="substrate-node-template",
                type_registry={
                    "types": {
                        "Record": "Vec<u8>",
                        "Parameter": "Bool",
                        "LaunchParameter": "Bool",
                        "<T as frame_system::Config>::AccountId": "AccountId",
                        "RingBufferItem": {
                            "type": "struct",
                            "type_mapping": [
                                ["timestamp", "Compact<u64>"],
                                ["payload", "Vec<u8>"],
                            ],
                        },
                        "RingBufferIndex": {
                            "type": "struct",
                            "type_mapping": [
                                ["start", "Compact<u64>"],
                                ["end", "Compact<u64>"],
                            ],
                        },
                    }
                },
            )
            logging.info("Successfully established connection to substrate node")
            return substrate
        except Exception as e:
            logging.error(f"Failed to connect to substrate: {e}")
            return None

    def __write_datalog(self, data: str) -> str or None:
        """
        Write any string to datalog
        Parameters
        ----------
        data : data to be stored as datalog
        Returns
        -------
        Hash of the datalog transaction
        """

        # create keypair
        try:
            keypair = Keypair.create_from_mnemonic(self.seed, ss58_format=32)
        except Exception as e:
            logging.error(f"Failed to create keypair for recording datalog: \n{e}")
            return None

        try:
            logging.info("Creating substrate call for recording datalog")
            call = self.substrate.compose_call(
                call_module="Datalog",
                call_function="record",
                call_params={"record": data},
            )
            logging.info(f"Successfully created a call for recording datalog:\n{call}")
            logging.info("Creating extrinsic for recording datalog")
            extrinsic = self.substrate.create_signed_extrinsic(
                call=call, keypair=keypair
            )
        except Exception as e:
            logging.error(f"Failed to create an extrinsic for recording datalog: {e}")
            return None

        try:
            logging.info("Submitting extrinsic for recording datalog")
            receipt = self.substrate.submit_extrinsic(
                extrinsic, wait_for_inclusion=True
            )
            logging.info(
                f"Extrinsic {receipt.extrinsic_hash} for recording datalog sent and included in block "
                f"{receipt.block_hash} "
            )
            return receipt.extrinsic_hash
        except Exception as e:
            logging.error(f"Failed to submit extrinsic for recording datalog: {e}")
            return None


    def send_data(self) -> None:
        """
        send data to frontier parachain "robonomics" from all devices
        """
        logging.info(f"send data from account: {self.name}")
        if self.sensor_id in Account.data_id:
            account_thread = threading.Thread(
                target=self.__write_datalog, args=(Account.data_id[self.sensor_id],)
            )
            account_thread.start()
        else:
            logging.warning(f"There is not sensor with id: {self.sensor_id}")


if __name__ == "__main__":
    # set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    # read config file
    config = read_config("config/config.yaml")

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
    time.sleep(2)

    # get all accounts
    accounts_list = list(config["accounts"])
    # create Account's inctance for every account in config
    for x in range(len(accounts_list)):
        accounts_list[x] = Account(accounts_list[x], config)

    while True:
        try:
            threads_num = threading.active_count()
            if threads_num > 25:
                logging.warning("Too many opened sending requests, waiting")
                time.sleep(12)
            for account in accounts_list:
                account.send_data()
            time.sleep(10)
        except KeyboardInterrupt:
            channel.join(timeout=5)
            connection.close()
            break
