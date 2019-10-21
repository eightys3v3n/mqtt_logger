from paho.mqtt.client import Client
from collections import defaultdict
import json
import sys
import sqlite3


global database, SUBS
database = defaultdict(lambda:[])
IGNORE_TOPICS = (
    "relay/0/set",
)
IGNORE_TOPICS = tuple("SonoffS31/"+i for i in IGNORE_TOPICS)

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


def set_host(name: str=None, ip: str=None, desc: str=None, ssid: str=None, mac: str=None, rssi: int=None):
    """Adds a host to the database. Returns True if failed, False if succeeded."""
    raise NotImplemented()


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(f"Connection failed: {rc}")
    print("Connected")
    client.subscribe("#")


def on_message(client, userdata, msg):
    if msg.topic in IGNORE_TOPICS:
        print(f"Ignoring msg from topic {msg.topic}")
    else:
        print(f"{msg.datetime} - {msg.topic}: {str(msg.payload.decode())}")
        database[msg.topic].append(msg.payload.decode())


def read_info(path):
    data = json.loads(open(path, 'r').read())
    return data


def main():
    global database

    client = Client()
    client.on_connect = on_connect
    client.on_message = on_message

    creds, host = read_info("secret.json")

    client.username_pw_set(*creds)
    r = client.connect(*host, 60)

    try:
        client.loop_forever()
    except KeyboardInterrupt: pass
    finally:
        client.disconnect()
        with open('database.json', 'w') as f:
            f.write(json.dumps(database))


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
