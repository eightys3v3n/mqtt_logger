from paho.mqtt.client import Client
from multiprocessing import Queue
from collections import defaultdict
import json
import sys
import sqlite3
from pathlib import Path


global database, SUBS

"""Topics to ignore, n matches any root topic.
This allows ignoring relay/0/set coming from any root topic (any device)."""
IGNORE_TOPICS = lambda root:(
    root+"/relay/0/set",
)

"""Command to create the database, this is run every time the database is opened.
So you need the [IF NOT EXISTS] part."""
CREATE_TABBLES = [
"""CREATE TABLE [IF NOT EXISTS]
host(
    name        VARCHAR(128) PRIMARY KEY,
    IP          VARCHAR(15),
    description VARCHAR(256),
    SSID        VARCHAR(64),
    MAC         VARCHAR(17),
    RSSI        INT)""",
"""CREATE TABLE [IF NOT EXISTS]
state(
    datetime    DATETIME PRIMARY KEY,
    FOREIGN KEY (host_name) REFERENCES host(name),
    state       BOOLEAN,
    current     DECIMAL,
    voltage     DECIMAL,
    power       DECIMAL,
    energy      DECIMAL)"""]

"""
The topics for all the SQL table columns, same order as the table creation command above.

host        str
ip          str
desc        str
ssid        str
mac         str
rssi        int

datetime    datetime
host        str
relay/0     boolean
current     float
voltage     int
power       float
energy      float
"""


def open_database(path: str):
    global database

    database = sqlite3.connect(path)
    for create_table in CREATE_TABLES:
        database.execute(create_table)
    database.commit()


def close_database():
    global database

    database.commit()
    database.close()


"""
This is an example command to update or add a value.

update test set name='john' where id=3012
IF @@ROWCOUNT=0
   insert into test(name) values('john');
"""


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(f"Connection failed: {rc}")
    else:
        print("Connected")
        client.subscribe("#")
    return rc


class Message:
    def __init__(self, msg):
        self.topic = msg.topic
        self.payload = msg.payload


def on_message(client, userdata, msg):
    global messages_in

    print()
    if msg.topic in IGNORE_TOPICS(msg.topic.split('/')[0]):
        print(f"Ignoring msg from topic {msg.topic}")
    else:
        #print("{}: {}".format(msg.topic, str(msg.payload.decode())))
        messages_in.put(Message(msg))


def save_message(msg):
    """Handles how to save the msg contents into the SQLite database."""
    raise NotImplemented()


def loop():
    global messages_in

    while True:
        msg = messages_in.get()
        print("{}: {}".format(msg.topic, str(msg.payload.decode())))
        save_message(msg)


def read_info(path):
    data = json.loads(open(path, 'r').read())
    return data


def main():
    global database
    global messages_in
    messages_in = Queue()

    database = open_database("database.sqlite")

    client = Client()
    client.on_connect = on_connect
    client.on_message = on_message

    if not Path("secret.json").exists():
        raise Exception("Need to create secret.json as stated in the README.md")

    creds, host = read_info("secret.json")

    if creds[0] != "" and creds[1] != "":
        client.username_pw_set(*creds)

    client.connect(*host, 60)

    try:
        client.loop_start()
        loop()
    except KeyboardInterrupt: pass
    finally:
        client.disconnect()
        close_database()
        


if __name__ == '__main__':
    main()




