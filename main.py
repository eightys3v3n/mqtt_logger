from paho.mqtt.client import Client
from multiprocessing import Queue
from collections import defaultdict
from datetime import datetime, timedelta
import json
import sys
from pathlib import Path
from db_helpers import *
<<<<<<< HEAD
import mysql.connector
import logging
import modules as __modules__
=======
from logging_setup import create_logger
>>>>>>> e1a9429 (implemented logging from another project with config file)


global database, SUBS

mqtt_logger = None
CONFIG_FILE = "secret.json" # MQTT host & creds, SQL creds
# This controls the number of messages that will be held if the database is taking too long.
# After this is exhausted new messages will not be logged until the processor frees up a spot.
MESSAGE_QUEUE_SIZE = 1024
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
<<<<<<< HEAD
=======
DEBUG_PRINT_SQL = False
CONFIG_FILE = "secret.json" # MQTT host & credentials, SQL credentials.
logger = create_logger('Main')
>>>>>>> e1a9429 (implemented logging from another project with config file)


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
<<<<<<< HEAD
        mqtt_logger.warning(f"Connection failed: {rc}")
    else:
        mqtt_logger.info("Connected")
        client.subscribe("#")
        mqtt_logger.debug("Subscribed to '#'")
    return rc
=======
        logger.info(f"Connection to MQTT failed: {rc}")
        return rc
    else:
        client.subscribe('#', qos=2)
        logger.info("Connected to MQTT")
        return 0
>>>>>>> e1a9429 (implemented logging from another project with config file)


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

<<<<<<< HEAD
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
=======
    logger.debug("Received message '{}':'{}'".format(msg.topic, msg.payload))
    messages_in.put(Message(msg))
    

def save_message(msg):
    logger.debug("Saving message {} {}/{}:{}".format(msg.datetime, msg.root, msg.topic, msg.payload))

    for m in modules():
        if '#' in m.ACCEPTED_TOPIC_PREFIXES:
            logger.debug("Sending message to module '{}'".format(m))
            m.save_message(msg)
            continue
        for t in m.ACCEPTED_TOPIC_PREFIXES:
            if m.topic.startswith(t):
                logger.debug("Sending message to module '{}'".format(m))
>>>>>>> e1a9429 (implemented logging from another project with config file)
                m.save_message(msg)
        else:
            for t in m.ACCEPTED_TOPIC_ROOTS:
                if msg.root == t:
                    logging.debug("  Sending message '{}'".format(msg))
                    m.save_message(msg)

def loop():
    global messages_in

    while True:
<<<<<<< HEAD
        save_message(messages_in.get())
=======
        msg = messages_in.get()
        logger.debug("Logging message: {}:{}".format(msg.topic, msg.payload))
        save_message(msg)
>>>>>>> e1a9429 (implemented logging from another project with config file)


def read_conn_details(path):
    """Returns ((mqtt_creds, mqtt_host), sql_creds)"""
    if not Path(path).exists():
        raise Exception("Need to create secret.json from secret.json.example as stated in the README.md")
    
    data = json.loads(open(path, 'r').read())
    data = (data['mqtt'], data['sql'])

    return data


def main():
    global messages_in, logger, mqtt_logger

<<<<<<< HEAD
    logging.basicConfig(level=logging.INFO)
    
    mqtt_logger = logging.getLogger("mqtt")
    mqtt_logger.setLevel(logging.WARNING)
    
    messages_in = Queue(maxsize=MESSAGE_QUEUE_SIZE)
    client = Client()
=======
    messages_in = Queue()
    client = Client(client_id="Logger {}".format(datetime.now().strftime("%Y%m%d%H%M")))
>>>>>>> e1a9429 (implemented logging from another project with config file)
    client.on_connect = on_connect
    client.on_message = on_message
    (mqtt_creds, mqtt_host), sql_creds = read_conn_details(CONFIG_FILE)

<<<<<<< HEAD
    logging.info("Connecting to MQTT host: {}".format(mqtt_host))
=======
    logger.debug("Hello from debug")
    
    logger.info("Connecting to MQTT host: {}".format(mqtt_host))
>>>>>>> e1a9429 (implemented logging from another project with config file)
    if mqtt_creds[0] != "" and mqtt_creds[1] != "":
        client.username_pw_set(*mqtt_creds)
    client.connect(*mqtt_host, 60)

    try:
        open_database(*sql_creds)
        for m in modules():
<<<<<<< HEAD
            logging.info("Initiating module {}".format(m.__name__.replace("modules.","")))
=======
            logger.info("Initiating module {}".format(m.__name__.replace("modules.", "")))
>>>>>>> e1a9429 (implemented logging from another project with config file)
            m.init(database)

        logger.info("Starting MQTT client...")
        client.loop_start()
<<<<<<< HEAD

        logging.info("Starting logger loop...")
        loop()
    except KeyboardInterrupt: pass
    finally:
        client.disconnect()
        close_database()
=======
        logger.info("Starting logger loop...")
        loop()
    except KeyboardInterrupt:
        logger.info("Quitting from keyboard interrupt")
    finally:
        client.disconnect()
        try:
            close_database(database)
        except UnboundLocalError: pass
>>>>>>> e1a9429 (implemented logging from another project with config file)
        

if __name__ == '__main__':
    main()




