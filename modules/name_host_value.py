from db_helpers import db_execute
from datetime import datetime
from main import Message
from logging_setup import create_logger
from math import isnan


"""
Logs any MQTT messages specified here that match these criteria:
- Dedicated table in mqtt_logger database with the structure outlined below.
- MQTT root topic is the same as the table name.
- 2nd part of the MQTT topic is the host name. (temperatures/<host_name>)
- Message payload is a float.
"""


logger = create_logger('Modules.NameHostValue')

# SQL table names, MQTT root topics, and column name inside respective table.
TABLE_NAMES = ['temperatures', 'relative_humidities', 'total_volatile_organic_compounds']

"""Commands to create the database, this is run every time the database is opened.
So you need the [IF NOT EXISTS] part. These are some example commands."""
CREATE_TABLES = [
"""CREATE TABLE IF NOT EXISTS {} (
    datetime    DATETIME NOT NULL,
    host_name   VARCHAR(128) NOT NULL,
    value       DECIMAL(30,15),
    PRIMARY KEY (datetime, host_name)
)""".format(t) for t in TABLE_NAMES]

ACCEPTED_TOPIC_PREFIXES = TABLE_NAMES


def init():
    """This is run once the database is connected. Do anything that needs to be done only once per script execution here."""
    for create_table in CREATE_TABLES:
        db_execute(create_table)


def update(dt: datetime, host_name: str, column: str, value: float):
    if column not in TABLE_NAMES:
        logger.error(f"Invalid column name specified: {column}")
    else:
        cmd = "INSERT INTO {0}(datetime, host_name, value) VALUES (%s, %s, %s)".format(column)
        db_execute(cmd, (dt.strftime("%Y-%m-%d %H-%M-%S.%f"), host_name, value))


def save_message(msg: Message):
    """This is passed every relevent message that MQTT receives."""
    field = msg.topic.split('/')[0]
    host_name = msg.topic.split('/')[-1]
    value = float(msg.payload)

    if isnan(value):
        logger.warning(f"Received NaN value for {field} from host:{host_name}")
        return

    logger.debug("Recording {} to column:{} for host:{}".format(value, field, host_name))

    """ This is improper and leads to inaccuracies when starting the client.
    MQTT will resend messages for the last so many minutes if they weren't delivered.
    So if this was stopped for a day, a bunch of out of date messages will come in at the
    same time and be given the same date and time.
    Ideally we would chagne the temperature monitor so the data it sends is a string containing
    the measurement date and time. However, then we are also relying on the datetime on the device.
    We could also change the QOS of the monitor device so the server Doesn't keep stagnent messages
    Good enough for now though."""
    dt = datetime.now()

    update(dt, host_name, field, value)
