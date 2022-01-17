from multiprocessing import Queue
import modules as __modules__
import json
from pathlib import Path
import config
from datetime import datetime
from db_helpers import *
import mqtt_helpers as mqtt
from logging_setup import create_logger


logger = create_logger('Main')


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
        logger.warning(f"Connection failed: {rc}")
    else:
        logger.info("Connected")
        client.subscribe("#")
        logger.debug("Subscribed to '#'")
    return rc


class Message:
    def __init__(self, msg):
        self.topic = msg.topic
        if isinstance(msg.payload, str):
            self.payload = msg.payload.decode()
        elif isinstance(msg.payload, bytes):
            self.payload = msg.payload.decode()
        else:
            self.payload = msg.payload
        self.datetime = datetime.now()
       
    def datetime_str(self):
        return self.datetime.strftime(config.General.DateTimeFormat)

    def __str__(self):
        return "'{}'/'{}' = '{}'".format(self.root, self.topic, self.payload)


def on_message(client, userdata, msg):
    global messages_in

    logger.info("Received message '{}':{}".format(msg.topic, msg.payload.decode()))

    try:
        messages_in.put(Message(msg), block=False)
    except Queue.Full:
        logger.warning("Message queue appears to be full, dropping message")


def save_message(msg):
    logger.debug("Saving message {} {}:{}".format(msg.datetime, msg.topic, msg.payload))

    for m in modules():
        try:
            if '#' in m.ACCEPTED_TOPIC_PREFIXES:
                logger.debug("Sending message to module '{}'".format(m))
                m.save_message(msg)
                continue
            for t in m.ACCEPTED_TOPIC_PREFIXES:
                if msg.topic.startswith(t):
                    logger.debug("Sending message to module '{}'".format(m))
                    m.save_message(msg)
        except Exception as e:
            logger.error(f"Exception raised by module {m}")
            logger.error(e)

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

    (mqtt_creds, mqtt_host), sql_creds = data
    return (mqtt_host, mqtt_creds), sql_creds


def main():
    global messages_in

    messages_in = Queue()
    mqtt.create(client_id="Logger {}".format(datetime.now().strftime("%Y%m%d%H%M")),
                 on_connect=on_connect,
                 on_message=on_message)

    (mqtt_host, mqtt_creds), sql_creds = read_conn_details(config.General.SecretFile)

    logger.info("Connecting to MQTT host: {}".format(mqtt_host))

    if mqtt_creds[0] != "" and mqtt_creds[1] != "":
        mqtt.username_pw_set(*mqtt_creds)
    mqtt.connect(host=mqtt_host[0],
                 port=mqtt_host[1])

    try:
        open_database(*sql_creds)
        for m in modules():
            logger.info("Initiating module {}".format(m.__name__.replace("modules.", "")))
            m.init()

        logger.info("Starting MQTT client...")
        mqtt.loop_start()
        logger.info("Starting logger loop...")
        loop()
    except KeyboardInterrupt:
        logger.info("Quitting from keyboard interrupt")
    finally:
        mqtt.disconnect()
        try:
            close_database()
        except UnboundLocalError: pass
        

if __name__ == '__main__':
    main()




