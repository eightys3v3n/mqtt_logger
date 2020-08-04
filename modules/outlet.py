from datetime import datetime, timedelta
from multiprocessing import Queue
from db_helpers import db_execute
from mysql.connector.errors import *
from main import DATETIME_FORMAT
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
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


"""Topics to accept and sub topics to ignore"""
ACCEPTED_TOPIC_ROOTS = [
    "outlets",
]
IGNORE_TOPICS = lambda root:(
    root+"/relay/0/set",
)
supported_stats = None


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


def init(database):
    global supported_stats
    
    for create_table in CREATE_TABLES:
        db_execute(create_table)
    supported_stats = get_supported_stats()


def get_supported_stats():
    res = db_execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stat'")
    stats = tuple(c[0] for c in res.fetchall())
    return stats


def host_update(host_name: str, column: str, data):
    cmd = "INSERT IGNORE INTO host(name) VALUES(%s)"
    db_execute(cmd, (host_name,))
    if column is not None:
        cmd = "UPDATE host SET {}=%s WHERE name=%s".format(column)
        db_execute(cmd, (data, host_name))


def get_latest_row(host_name):
    cmd = "SELECT datetime, state, current, voltage, power, energy FROM stat WHERE host_name=%s ORDER BY datetime desc LIMIT 1"
    res = db_execute(cmd, (host_name,))
    res = res.fetchone()
    return res


def get_last_state(host_name):
    cmd = "SELECT state FROM stat WHERE host_name=%s AND state IS NOT NULL ORDER BY datetime desc LIMIT 1"
    res = db_execute(cmd, (host_name,))
    res = res.fetchone()
    
    if res is not None and len(res) == 1:
        return res[0]
    else:
        return None


def carry_last_state(host_name: str, dt: str):
    last_state = get_last_state(host_name)
    cmd = "UPDATE stat SET state=%s WHERE host_name=%s AND datetime=%s"
    db_execute(cmd, (last_state, host_name, dt))
    logger.debug("Carried last state")


def update_stat(dt: datetime, host_name: str, column: str, data):
    global supported_stats
    
    if supported_stats is None:
        logging.warning("Attempted to log message before init-ing the database with outlet.init_database()")
        return
        
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
    except IntegrityError as e:
        if str(e).startswith("1062"):
            print("Tried to add this entry twice?", e, "\n", cmd, sql_data)
        else:
            host_update(host_name, None, None)
            db_execute(cmd, sql_data)

    if column != "state":
        carry_last_state(host_name, sql_data[-1])


def save_message(msg):
    """Handles how to save the msg contents into the SQLite database."""
    host_name = msg.topic.split('/')[0]
    topic = '/'.join(msg.topic.split('/')[1:])
    
    logger.debug("Processing message '{}':'{}'".format(topic, msg.payload))

    if topic == "ip":
        host_update(host_name, "IP", msg.payload)
    elif topic == "desc":
        host_update(host_name, "description", msg.payload)
    elif topic == "ssid":
        host_update(host_name, "SSID", msg.payload)
    elif topic == "mac":
        host_update(host_name, "MAC", msg.payload)
    elif topic == "rssi":
        host_update(host_name, "RSSI", msg.payload)

    elif topic == "relay/0":
        update_stat(msg.datetime, host_name, "state",
                True if msg.payload == '1' else False)
    elif topic == "current":
        update_stat(msg.datetime, host_name, "current",
                float(msg.payload))
    elif topic == "voltage":
        update_stat(msg.datetime, host_name, "voltage",
                float(msg.payload))
    elif topic == "power":
        update_stat(msg.datetime, host_name, "power",
                float(msg.payload))
    elif topic == "energy":
        update_stat(msg.datetime, host_name, "energy",
                float(msg.payload))
