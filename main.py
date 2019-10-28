from paho.mqtt.client import Client
from multiprocessing import Queue
from collections import defaultdict
from datetime import datetime
import json
import sys
import sqlite3
from pathlib import Path


global database, SUBS

DATABASE_PATH = Path("/var/local/mqtt_logger/database.sqlite")
COMMIT_INTERVAL = 10 # minutes
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
state(
    datetime    DATETIME2 PRIMARY KEY,
    host_name   VARCHAR(128),
    state       BOOLEAN,
    FOREIGN KEY (host_name) REFERENCES host(name)
)""",
"""CREATE TABLE IF NOT EXISTS
current(
    datetime    DATETIME2 PRIMARY KEY,
    host_name   VARCHAR(128),
    current     DECIMAL,
    FOREIGN KEY (host_name) REFERENCES host(name)
)""",
"""CREATE TABLE IF NOT EXISTS
voltage(
    datetime    DATETIME2 PRIMARY KEY,
    host_name   VARCHAR(128),
    voltage     DECIMAL,
    FOREIGN KEY (host_name) REFERENCES host(name)
)""",
"""CREATE TABLE IF NOT EXISTS
power(
    datetime    DATETIME2 PRIMARY KEY,
    host_name   VARCHAR(128),
    power       DECIMAL,
    FOREIGN KEY (host_name) REFERENCES host(name)
)""",
"""CREATE TABLE IF NOT EXISTS
energy(
    datetime    DATETIME2 PRIMARY KEY,
    host_name   VARCHAR(128),
    energy      DECIMAL,
    FOREIGN KEY (host_name) REFERENCES host(name)
)"""]

last_commit = datetime.now()

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
    database = sqlite3.connect(path)
    for create_table in CREATE_TABLES:
        database.execute(create_table)
    database.commit()
    return database


def close_database():
    global database

    database.commit()
    database.close()


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
        return self.datetime.strftime("%Y-%m-%d %H:%M:%S.%f")


def on_message(client, userdata, msg):
    global messages_in

    if msg.topic in IGNORE_TOPICS(msg.topic.split('/')[0]):
        print(f"Ignoring msg from topic {msg.topic}")
    else:
        #print("{}: {}".format(msg.topic, str(msg.payload.decode())))
        messages_in.put(Message(msg))


def db_execute(cmd, data):
    global database
    if DEBUG_PRINT_SQL:
        print(cmd, data)
    database.execute(cmd, data)


def host_update(host_name: str, column: str, data):
    cmd = "INSERT OR IGNORE INTO host(name) VALUES(?)"
    db_execute(cmd, (host_name,))
    cmd = "UPDATE host SET {}=? WHERE name=?".format(column)
    db_execute(cmd, (data, host_name))


def update(table: str, datetime: datetime, host_name: str, column: str, data):
    cmd = "INSERT INTO {}(datetime, host_name, {}) VALUES(?, ?, ?)".format(table, column)
    db_execute(cmd, (datetime, host_name, data))


def periodic_commit():
    global last_commit, database
    r = datetime.now() - last_commit
    minutes = (r.seconds//60)%60
    if minutes >= COMMIT_INTERVAL:
        print("{}: Committing database.".format(datetime.now()))
        database.commit()
        last_commit = datetime.now()


def save_message(msg):
    """Handles how to save the msg contents into the SQLite database."""
    # handle msg.root
    if msg.topic == "ip":
        host_update(msg.root, "IP", msg.payload)
    elif msg.topic == "desc":
        host_update(msg.root, "description", msg.payload)
    elif msg.topic == "ssid":
        host_update(msg.root, "SSID", msg.payload)
    elif msg.topic == "mac":
        host_update(msg.root, "MAC", msg.payload)
    elif msg.topic == "rssi":
        host_update(msg.root, "RSSI", msg.payload)

    elif msg.topic == "relay/0":
        update("state", msg.datetime_str(), msg.root, "state",
                True if msg.payload == '1' else False)
    elif msg.topic == "current":
        update("current", msg.datetime_str(), msg.root, "current",
                float(msg.payload))
    elif msg.topic == "voltage":
        update("voltage", msg.datetime_str(), msg.root, "voltage",
                float(msg.payload))
    elif msg.topic == "power":
        update("power", msg.datetime_str(), msg.root, "power",
                float(msg.payload))
    elif msg.topic == "energy":
        update("energy", msg.datetime_str(), msg.root, "energy",
                float(msg.payload))
    periodic_commit()


def loop():
    global messages_in

    while True:
        msg = messages_in.get()
        print("{}: {}".format(msg.topic, msg.payload))
        save_message(msg)


def read_info(path):
    data = json.loads(open(path, 'r').read())
    return data


def main():
    global database
    global messages_in
    messages_in = Queue()

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
        database = open_database(DATABASE_PATH)
        client.loop_start()
        loop()
    except KeyboardInterrupt: pass
    finally:
        client.disconnect()
        close_database()
        


if __name__ == '__main__':
    main()




