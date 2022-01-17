from datetime import datetime, timedelta
from db_helpers import db_execute
from logging_setup import create_logger
import config
import sql_templates


"""
Setup to record device info from Sonoff S31 outlets running Espurna.
"""


logger = create_logger('Modules.DevInfo')


"""Command to create the database, this is run every time the database is opened.
So you need the [IF NOT EXISTS] part."""
CREATE_TABLES = [sql_templates.Hosts]


# Topics to accept and sub topics to ignore
ACCEPTED_TOPIC_PREFIXES = [
    "espurna",
]


"""
The topics for all the SQL table columns, same order as the table creation command above.

datetime    datetime
host        str
ip          str
desc        str
ssid        str
mac         str
rssi        int
"""


def init():
    for create_table in CREATE_TABLES:
        db_execute(create_table)


def host_update(date_time: str, host_name: str, column: str, data):
    cmd = "INSERT IGNORE INTO hosts(last_updated, name) VALUES(%s, %s)"
    db_execute(cmd, (date_time, host_name,))
    if column is not None:
        cmd = "UPDATE hosts SET last_updated=%s, {}=%s WHERE name=%s".format(column)
        db_execute(cmd, (date_time, data, host_name))

    logger.info(f"Updated {column} to {data} stats for {host_name}")


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
    logger.debug("Received message {}:{}".format(msg.topic, msg.payload))

    host_name = get_hostname(msg.topic)
    logger.debug(f"  Hostname:{host_name}")

    column = get_column(msg.topic)
    logger.debug(f"  Column:{column}")

    if column in ('status', 'app','version','board','host','uptime','datetime',
                 'freeheap','loadavg','vcc','relay','reactive','apparent','factor','set',
                 'current','voltage','power','temperature',
                 'reactive','apparent','factor','energy','0'):
        pass
    elif column == "ip":
        host_update(msg.datetime_str(), host_name, "IP", msg.payload)
    elif column == "desc":
        host_update(msg.datetime_str(), host_name, "description", msg.payload)
    elif column == "ssid":
        host_update(msg.datetime_str(), host_name, "SSID", msg.payload)
    elif column == "mac":
        host_update(msg.datetime_str(), host_name, "MAC", msg.payload)
    elif column == "rssi":
        host_update(msg.datetime_str(), host_name, "RSSI", msg.payload)
    else:
        logger.warn("Received a message that couldn't be saved: {}".format(msg.topic))
