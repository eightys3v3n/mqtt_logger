from paho.mqtt.client import Client
from multiprocessing import Queue
from collections import defaultdict
from datetime import datetime, timedelta
import json
import sys
from pathlib import Path
import mysql.connector


global database, SUBS

DEBUG_PRINT_SQL = False
"""Topics to ignore, n matches any root topic.
This allows ignoring relay/0/set coming from any root topic (any device)."""
IGNORE_TOPICS = lambda root:(
    root+"stufff",
)

"""Command to create the database, this is run every time the database is opened.
So you need the [IF NOT EXISTS] part."""
CREATE_TABLES = [
"""CREATE TABLE IF NOT EXISTS
screen_time(
    screen_on      DATETIME NOT NULL,
    screen_off     DATETIME NOT NULL,
    PRIMARY KEY (screen_on, screen_off)
)"""]

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TASKER_DATETIME_FORMAT = "%Y-%m-%d %H.%M"


def open_database(user: str, password: str):
    global database
    database = mysql.connector.connect(host="127.0.0.1", user=user, passwd=password, database="mqtt_logger", autocommit=True)
    cursor = database.cursor()
    for create_table in CREATE_TABLES:
        db_execute(create_table)
    database.commit()


def close_database():
    global database

    try:
        database.commit()
        database.close()
    except NameError: pass


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


def db_execute(cmd, data=None):
    global database
    cursor = database.cursor()
    if DEBUG_PRINT_SQL:
        print(cmd, data)
    if data is not None:
        cursor.execute(cmd, data)
    else:
        cursor.execute(cmd)
    return cursor


def db_execute_many(cmd, data):
    global database
    cursor = database.cursor()
    #logger.debug("SQL command: {}\n\t\tSQL data: {}".format(cmd, data))
    try:
        cursor.executemany(cmd, data)
    except SQLErrors.IntegrityError as e:
        if not e.__str__().startswith("1062 (23000): Duplicate entry"):
            raise e
    return cursor


def add_times(msg: str):
    raw_times = msg.split(';')
    times = []
    for rt in raw_times:
        rt = rt.split(',')
        rt = (datetime.strptime(rt[0], TASKER_DATETIME_FORMAT), datetime.strptime(rt[1], TASKER_DATETIME_FORMAT))
        times.append(rt)
    if len(times) > 0:
        cmd = "INSERT INTO screen_time VALUES(%s, %s)".format(*rt)
        sql_data = times
        db_execute_many(cmd, sql_data)


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
        db_execute(cmd, sql_data)
    except mysql.connector.errors.IntegrityError as e:
        if str(e).startswith("1062"):
            print("Tried to add this entry twice?", e, "\n", cmd, sql_data)
        else:
            host_update(host_name, None, None)
            db_execute(cmd, sql_data)

    if column != "state":
        carry_last_state(host_name, sql_data[-1])


def save_message(msg):
    """Handles how to save the msg contents into the SQLite database."""
    if msg.root == "PhoneScreen":
        print("Adding times: {}".format(msg.payload))
        add_times(msg.payload)


def loop():
    global messages_in

    while True:
        msg = messages_in.get()
        #print("{}: {}".format(msg.topic, msg.payload))
        save_message(msg)


def read_info(path):
    data = json.loads(open(path, 'r').read())
    data = (data['mqtt'], data['sql'])
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

    (mqtt_creds, mqtt_host), sql_creds = read_info("secret.json")

    if mqtt_creds[0] != "" and mqtt_creds[1] != "":
        client.username_pw_set(*mqtt_creds)

    client.connect(*mqtt_host, 60)

    try:
        open_database(*sql_creds)
        
        client.loop_start()
        loop()
    except KeyboardInterrupt: pass
    finally:
        client.disconnect()
        close_database()
        

if __name__ == '__main__':
    main()




