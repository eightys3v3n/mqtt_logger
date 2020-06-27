from paho.mqtt.client import Client
from multiprocessing import Queue
from collections import defaultdict
from datetime import datetime, timedelta
import json
import sys
from pathlib import Path
from db_helpers import *
import mysql.connector


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


# def open_database(user: str, password: str):
#     global database
#     database = mysql.connector.connect(host="127.0.0.1", user=user, passwd=password, database="mqtt_logger", autocommit=True)
#     cursor = database.cursor()
#     for create_table in CREATE_TABLES:
#         db_execute(create_table)
#     database.commit()


def get_supported_stats():
    global database
    cursor = database.cursor()
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stat'")
    stats = tuple(c[0] for c in cursor.fetchall())
    return stats


# def close_database():
#     global database

#     try:
#         database.commit()
#         database.close()
#     except NameError: pass


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


# def db_execute(cmd, data=None):
#     global database
#     cursor = database.cursor()
#     if DEBUG_PRINT_SQL:
#         print(cmd, data)
#     if data is not None:
#         cursor.execute(cmd, data)
#     else:
#         cursor.execute(cmd)
#     return cursor


def host_update(host_name: str, column: str, data):
    cmd = "INSERT IGNORE INTO host(name) VALUES(%s)"
    db_execute(database, cmd, (host_name,))
    if column is not None:
        cmd = "UPDATE host SET {}=%s WHERE name=%s".format(column)
        db_execute(database, cmd, (data, host_name))


def get_latest_row(host_name):
    cmd = "SELECT datetime, state, current, voltage, power, energy FROM stat WHERE host_name=%s ORDER BY datetime desc LIMIT 1"
    res = db_execute(database, cmd, (host_name,))
    res = res.fetchone()
    return res


def get_last_state(host_name):
    cmd = "SELECT state FROM stat WHERE host_name=%s AND state IS NOT NULL ORDER BY datetime desc LIMIT 1"
    res = db_execute(database, cmd, (host_name,))
    res = res.fetchone()
    
    if res is not None and len(res) == 1:
        return res[0]
    else:
        return None


def carry_last_state(host_name: str, dt: str):
    last_state = get_last_state(host_name)
    cmd = "UPDATE stat SET state=%s WHERE host_name=%s AND datetime=%s"
    db_execute(database, cmd, (last_state, host_name, dt))
    print("Carried last state")


def update_stat(dt: datetime, host_name: str, column: str, data):
    global supported_stats
    if column not in supported_stats:
        print("Ignoring unsupported stat '{}'".format(column))
        return

    latest = get_latest_row(host_name)
    
    if latest is None or latest[0] < dt - timedelta(seconds=3) or latest[0] > dt + timedelta(seconds=3):

        cmd = "INSERT INTO stat({}, host_name, datetime) VALUES(%s, %s, %s)".format(column)
        sql_data = (data, host_name, dt.strftime(DATETIME_FORMAT))
    
        print("Adding a new row, {}:{} {}={}".format(dt, host_name, column, data))
    
    else:
        cmd = "UPDATE stat SET {}=%s WHERE host_name=%s AND datetime=%s".format(column)
        sql_data = (data, host_name, latest[0].strftime(DATETIME_FORMAT))
    
        print("Updating existing row, {}:{} {}={}".format(latest[0], host_name, column, data))

    try:
        db_execute(database, cmd, sql_data)
    except mysql.connector.errors.IntegrityError as e:
        if str(e).startswith("1062"):
            print("Tried to add this entry twice?", e, "\n", cmd, sql_data)
        else:
            host_update(host_name, None, None)
            db_execute(database, cmd, sql_data)

    if column != "state":
        carry_last_state(host_name, sql_data[-1])


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
        update_stat(msg.datetime, msg.root, "state",
                True if msg.payload == '1' else False)
    elif msg.topic == "current":
        update_stat(msg.datetime, msg.root, "current",
                float(msg.payload))
    elif msg.topic == "voltage":
        update_stat(msg.datetime, msg.root, "voltage",
                float(msg.payload))
    elif msg.topic == "power":
        update_stat(msg.datetime, msg.root, "power",
                float(msg.payload))
    elif msg.topic == "energy":
        update_stat(msg.datetime, msg.root, "energy",
                float(msg.payload))


def loop():
    global messages_in

    while True:
        msg = messages_in.get()
        print("{}: {}".format(msg.topic, msg.payload))
        save_message(msg)


def read_conn_details(path):
    """Returns ((mqtt_creds, mqtt_host), sql_creds)"""
    if not Path(CONFIG_FILE).exists():
        raise Exception("Need to create secret.json as stated in the README.md")
    
    data = json.loads(open(path, 'r').read())
    data = (data['mqtt'], data['sql'])

    return data


def main():
    global database
    global messages_in
    global supported_stats
    messages_in = Queue()

    client = Client()
    client.on_connect = on_connect
    client.on_message = on_message

    if not Path("secret.json").exists():
        raise Exception("Need to create secret.json as stated in the README.md")

    (mqtt_creds, mqtt_host), sql_creds = read_conn_details("secret.json")

    if mqtt_creds[0] != "" and mqtt_creds[1] != "":
        client.username_pw_set(*mqtt_creds)

    client.connect(*mqtt_host, 60)

    try:
        database = open_database(*sql_creds)
        supported_stats = get_supported_stats()
        
        client.loop_start()
        loop()
    except KeyboardInterrupt: pass
    finally:
        client.disconnect()
        close_database(database)
        

if __name__ == '__main__':
    main()




