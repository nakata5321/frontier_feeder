#!/usr/bin/env python3
import json
import time
import typing as tp
import threading
import logging
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


def callback(ch, method, properties, body):
    data = body.decode("utf-8")
    data_js = json.loads(data)
    data_id[data_js['sensor']['id']] = data
    ch.basic_ack(delivery_tag=method.delivery_tag)


def send_data(config: dict) -> None:
    """
    send data to frontier parachain "robonomics" from all devices
    """
    for account in config["accounts"].keys():
        substrate_datalog = substrate_connection(config["substrate_wss"])
        write_datalog(substrate_datalog, account["seed"], data_id[account["id"]])


def substrate_connection(url: str) -> tp.Any:
    """
    establish connection to a specified substrate node
    """
    try:
        logging.info("Establishing connection to substrate node")
        substrate = SubstrateInterface(
            url=url,
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
                    }
                }
            },
        )
        logging.info("Successfully established connection to substrate node")
        return substrate
    except Exception as e:
        logging.error(f"Failed to connect to substrate: {e}")
        return None


def write_datalog(substrate, seed: str, data: str) -> str or None:
    """
    Write any string to datalog
    Parameters
    ----------
    substrate : substrate connection instance
    seed : mnemonic seed of account which writes datalog
    data : data to be stored as datalog
    Returns
    -------
    Hash of the datalog transaction
    """

    # create keypair
    try:
        keypair = Keypair.create_from_mnemonic(seed, ss58_format=32)
    except Exception as e:
        logging.error(f"Failed to create keypair for recording datalog: \n{e}")
        return None

    try:
        logging.info("Creating substrate call for recording datalog")
        call = substrate.compose_call(
            call_module="Datalog",
            call_function="record",
            call_params={
                'record': data
            }
        )
        logging.info(f"Successfully created a call for recording datalog:\n{call}")
        logging.info("Creating extrinsic for recording datalog")
        extrinsic = substrate.create_signed_extrinsic(call=call, keypair=keypair)
    except Exception as e:
        logging.error(f"Failed to create an extrinsic for recording datalog: {e}")
        return None

    try:
        logging.info("Submitting extrinsic for recording datalog")
        receipt = substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True)
        logging.info(f"Extrinsic {receipt.extrinsic_hash} for recording datalog sent and included in block {receipt.block_hash}")
        return receipt.extrinsic_hash
    except Exception as e:
        logging.error(f"Failed to submit extrinsic for recording datalog: {e}")
        return None


if __name__ == '__main__':
    # set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    data_id = {}

    # read config file
    config = read_config("config/config.yaml")
    # print(len(config["accounts"].keys()))

    # Connect to RabbitMQ server
    logging.info("establishing connection to RabbitMQ")
    credential = pika.PlainCredentials(
        config["RabbitMQ"]["credentials"]["login"],
        config["RabbitMQ"]["credentials"]["passwrd"]
    )
    params = pika.ConnectionParameters(
        host=config["RabbitMQ"]["host"],
        port=int(config["RabbitMQ"]["port"]),
        virtual_host='/',
        heartbeat=60,
        credentials=credential
    )
    connection = pika.BlockingConnection(params)

    channel = connection.channel()
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume('aira-event', callback)
    try:
        channel.start_consuming()
        logging.info("Successfully established connection to RabbitMQ")
    except Exception as e:
        channel.stop_consuming()
        connection.close()
        logging.error(f"Failed to connect to RabbitMQ: {e}")

    while True:
        threads_num = threading.active_count()
        if threads_num > 20:
            logging.warning("Too many opened sending requests, waiting")
            time.sleep(12)
        send_datalog = threading.Thread(target=send_data)
        send_datalog.start()
        time.sleep(4)
