from datetime import datetime, timedelta
from multiprocessing import Queue
from db_helpers import db_execute
from logging_setup import create_logger
from functools import cache
import config


""" Currently expecting MQTT structure:
    outlets/<hostname>/<field>:<value>
"""


logger = create_logger('Modules.Outlet')


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


# Topics to accept and sub topics to ignore
ACCEPTED_TOPIC_PREFIXES = [
    "outlets",
]


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


def init():
    for create_table in CREATE_TABLES:
        db_execute(create_table)


@cache
def get_supported_stats():
    cursor = db_execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stat'")
    stats = tuple(c[0] for c in cursor.fetchall())
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
    if column not in get_supported_stats():
        logger.debug("Ignoring unsupported stat '{}'".format(column))
        return

    latest = get_latest_row(host_name)
    logger.debug("Latest row in DB for host_name {} is {}".format(host_name, latest))

    if latest is None or latest[0] < dt - timedelta(seconds=3) or latest[0] > dt + timedelta(seconds=3):
        cmd = "INSERT INTO stat({}, host_name, datetime) VALUES(%s, %s, %s)".format(column)
        sql_data = (data, host_name, dt.strftime(config.General.DateTimeFormat))
        logger.debug("Adding a new row, {}:{} {}={}".format(dt, host_name, column, data))
    else:
        cmd = "UPDATE stat SET {}=%s WHERE host_name=%s AND datetime=%s".format(column)
        sql_data = (data, host_name, latest[0].strftime(config.General.DateTimeFormat))
        logger.debug("Updating existing row, {}:{} {}={}".format(latest[0], host_name, column, data))

    try:
        db_execute(cmd, sql_data)
    except mysql.connector.errors.IntegrityError as e:
        logger.warn("Ingegrity error")

        if str(e).startswith("1062"):
            logger.warn("Tried to add this entry twice?", e, "\n", cmd, sql_data)
        else:
            host_update(host_name, None, None)
            db_execute(cmd, sql_data)

    if column != "state":
        carry_last_state(host_name, sql_data[-1])


def get_hostname(topic):
    """Parses the device hostname out of the topic"""
    return topic.split('/')[1]


def get_column(topic):
    """Parses the column of the message topic"""
    column = topic.split('/')[-1]
    if len(column) == 1:
        column = topic.split('/')[-2]
    return column


def save_message(msg):
    """Handles how to save the msg contents into the SQLite database."""
    logger.debug("Received message {}:'{}'".format(msg.topic, msg.payload))

    host_name = get_hostname(msg.topic)
    logger.debug(f"  Hostname {host_name}")

    column = get_column(msg.topic)
    logger.debug(f"  Column {column}")

    if column in ('status', 'app','version','board','host','uptime','datetime','freeheap','loadavg','vcc','relay','reactive','apparent','factor','set'):
        pass
    elif column == "ip":
        host_update(host_name, "IP", msg.payload)
    elif column == "desc":
        host_update(host_name, "description", msg.payload)
    elif column == "ssid":
        host_update(host_name, "SSID", msg.payload)
    elif column == "mac":
        host_update(host_name, "MAC", msg.payload)
    elif column == "rssi":
        host_update(host_name, "RSSI", msg.payload)

    elif column == "relay/0":
        update_stat(msg.datetime, host_name, "state",
                True if msg.payload == '1' else False)
    elif column == "current":
        update_stat(msg.datetime, host_name, "current",
                float(msg.payload))
    elif column == "voltage":
        update_stat(msg.datetime, host_name, "voltage",
                float(msg.payload))
    elif column == "power":
        update_stat(msg.datetime, host_name, "power",
                float(msg.payload))
    elif column == "energy":
        update_stat(msg.datetime, host_name, "energy",
                float(msg.payload))
    else:
        logger.warn("Received a message that couldn't be saved: {}".format(msg.topic))
