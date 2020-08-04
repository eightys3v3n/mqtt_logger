from paho.mqtt.client import Client
from multiprocessing import Queue
from collections import defaultdict
from datetime import datetime, timedelta
import json
import sys
from pathlib import Path
from db_helpers import *
import mysql.connector
import logging
import modules as __modules__


global database, SUBS

mqtt_logger = None
CONFIG_FILE = "secret.json" # MQTT host & creds, SQL creds
# This controls the number of messages that will be held if the database is taking too long.
# After this is exhausted new messages will not be logged until the processor frees up a spot.
MESSAGE_QUEUE_SIZE = 1024
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def modules():
    """
    Returns the module for each module imported in modules/__init__.
    Assumes everything that doesn't start with __ is a logging module.
    """
    for m in dir(__modules__):
        if not m.startswith("__"):
            yield __modules__.__dict__[m]


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        mqtt_logger.warning(f"Connection failed: {rc}")
    else:
        mqtt_logger.info("Connected")
        client.subscribe("#")
        mqtt_logger.debug("Subscribed to '#'")
    return rc


class Message:
    def __init__(self, msg):
        self.root = msg.topic.split('/')[0]
        self.topic = "/".join(msg.topic.split('/')[1:])
        if isinstance(msg.payload, str):
            self.payload = msg.payload
        elif isinstance(msg.payload, bytes):
            self.payload = msg.payload.decode()
        else:
            self.payload = msg.payload
        self.datetime = datetime.now()
       
    def datetime_str(self):
        return self.datetime.strftime(DATETIME_FORMAT)

    def __str__(self):
        return "'{}'/'{}' = '{}'".format(self.root, self.topic, self.payload)
    

def on_message(client, userdata, msg):
    global messages_in

    mqtt_logger.debug("Received message '{}':'{}'".format(msg.topic, str(msg.payload.decode())))
    try:
        messages_in.put(Message(msg), block=False)
    except Queue.Full:
        mqtt_logger.warning("Message queue appears to be full, dropping message")


def save_message(msg):
    """Handles how to save the msg contents into the SQLite database."""
    logging.debug("Saving message '{}/{}:{}'".format(msg.root, msg.topic, msg.payload))
    for m in modules():
        logging.debug(" Checking module '{}'".format(m.__name__.replace("modules.","")))

        if '#' in m.ACCEPTED_TOPIC_ROOTS:
            for ignore in m.IGNORE_TOPICS(msg.root):
                if ignore == "{}/{}".format(msg.root, msg.topic):
                    logging.debug("  Ignoring msg with equals rule '{}'".format(ignore))
                    break
                if "{}/{}".format(msg.root, msg.topic).startswith(ignore):
                    logging.debug("  Ignoring msg with starts with rule '{}'".format(ignore))
                    break
            else:
                logging.debug("  Sending message '{}'".format(msg))
                m.save_message(msg)
        else:
            for t in m.ACCEPTED_TOPIC_ROOTS:
                if msg.root == t:
                    logging.debug("  Sending message '{}'".format(msg))
                    m.save_message(msg)

def loop():
    global messages_in

    while True:
        save_message(messages_in.get())


def read_conn_details(path):
    """Returns ((mqtt_creds, mqtt_host), sql_creds)"""
    if not Path(path).exists():
        raise Exception("Need to create secret.json from secret.json.example as stated in the README.md")
    
    data = json.loads(open(path, 'r').read())
    data = (data['mqtt'], data['sql'])

    return data


def main():
    global messages_in, logger, mqtt_logger

    logging.basicConfig(level=logging.INFO)
    
    mqtt_logger = logging.getLogger("mqtt")
    mqtt_logger.setLevel(logging.WARNING)
    
    messages_in = Queue(maxsize=MESSAGE_QUEUE_SIZE)
    client = Client()
    client.on_connect = on_connect
    client.on_message = on_message
    (mqtt_creds, mqtt_host), sql_creds = read_conn_details(CONFIG_FILE)

    logging.info("Connecting to MQTT host: {}".format(mqtt_host))
    if mqtt_creds[0] != "" and mqtt_creds[1] != "":
        client.username_pw_set(*mqtt_creds)
    client.connect(*mqtt_host, 60)

    try:
        open_database(*sql_creds)
        for m in modules():
            logging.info("Initiating module {}".format(m.__name__.replace("modules.","")))
            m.init(database)

        logging.info("Starting MQTT client...")
        client.loop_start()

        logging.info("Starting logger loop...")
        loop()
    except KeyboardInterrupt: pass
    finally:
        client.disconnect()
        close_database()
        

if __name__ == '__main__':
    main()




