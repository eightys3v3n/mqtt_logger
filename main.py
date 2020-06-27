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

CONFIG_FILE = "secret.json" # MQTT host & creds, SQL creds
DEBUG_PRINT_SQL = False
"""Topics to ignore, n matches any root topic.
This allows ignoring relay/0/set coming from any root topic (any device)."""
IGNORE_TOPICS = lambda root:(
    root+"/relay/0/set",
)

"""Command to create the database, this is run every time the database is opened.
So you need the [IF NOT EXISTS] part."""
CREATE_TABLES = [
"""CREATE TABLE IF NOT EXISTS
host(
    name        VARCHAR(128) PRIMARY KEY,
    IP          VARCHAR(15),
    description VARCHAR(256),
    SSID        VARCHAR(64),
    MAC         VARCHAR(17),
    RSSI        INT
)""",
"""CREATE TABLE IF NOT EXISTS
stat(
    datetime    DATETIME NOT NULL,
    host_name   VARCHAR(128) NOT NULL,
    state       BOOLEAN,
    current     DECIMAL(30,15),
    voltage     DECIMAL(30,15),
    power       DECIMAL(30,15),
    energy      DECIMAL(30,15),
    FOREIGN KEY (host_name) REFERENCES host(name),
    PRIMARY KEY (datetime, host_name)
)"""]

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
        print(f"Connection failed: {rc}")
    else:
        print("Connected")
        client.subscribe("#")
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


def on_message(client, userdata, msg):
    global messages_in

    if msg.topic in IGNORE_TOPICS(msg.topic.split('/')[0]):
        print(f"Ignoring msg from topic {msg.topic}")
    else:
        #print("{}: {}".format(msg.topic, str(msg.payload.decode())))
        messages_in.put(Message(msg))


def save_message(msg):
    """Handles how to save the msg contents into the SQLite database."""
    logging.debug("Saving message '{}:{}'".format(msg.topic, msg.payload))
    for m in modules():
        if '#' in m.ACCEPTED_TOPIC_PREFIXES:
            logging.debug("Sending message to module '{}'".format(m.__name__.replace("modules.","")))
            m.save_message(msg)
        else:
            for t in m.ACCEPTED_TOPICS:
                if m.topic.startswith(t):
                    logging.debug("Sending message to module '{}'".format(m.__name__.replace("modules.","")))
                    m.save_message(msg)

def loop():
    global messages_in

    while True:
        msg = messages_in.get()
        logging.debug("Logging message: {}:{}".format(msg.topic, msg.payload))
        save_message(msg)


def read_conn_details(path):
    """Returns ((mqtt_creds, mqtt_host), sql_creds)"""
    if not Path(path).exists():
        raise Exception("Need to create secret.json from secret.json.example as stated in the README.md")
    
    data = json.loads(open(path, 'r').read())
    data = (data['mqtt'], data['sql'])

    return data


def main():
    global messages_in

    logging.basicConfig(level=logging.DEBUG)
    messages_in = Queue()
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




