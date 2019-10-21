from paho.mqtt.client import Client
from multiprocessing import Queue
from collections import defaultdict
import json
import sys
import sqlite3


global database, SUBS
database = defaultdict(lambda:[])
IGNORE_TOPICS = lambda n:(
    n+"/relay/0/set",
)

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
    FOREIGN KEY (mac) REFERENCES host(MAC),
    state       BOOLEAN,
    current     DECIMAL,
    voltage     DECIMAL,
    power       DECIMAL,
    energy      DECIMAL)"""]


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

    creds, host = read_info("secret.json")

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



"""
SQL table

relay/0     [0|1]
host        str
desc        str
ssid        str
ip          str
mac         str
rssi        int
datetime    datetime

current     float
voltage     int
power       float
energy      float







"""
