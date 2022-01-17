from db_helpers import db_execute
from datetime import datetime
from main import Message
from logging_setup import create_logger


"""Add the file name to the modules/__init__.py file for it to be used."""

logger = create_logger('Modules.Espurna')


"""Commands to create the database, this is run every time the database is opened.
So you need the [IF NOT EXISTS] part. These are some example commands."""
CREATE_TABLES = [
"""CREATE TABLE IF NOT EXISTS {} (
    datetime    DATETIME NOT NULL,
    host_name   VARCHAR(128) NOT NULL,
    temperatures DECIMAL(30,15),
    PRIMARY KEY (datetime, host_name)
)""".format('temperatures')]


"""Topics to accept, # means everything."""
ACCEPTED_TOPIC_PREFIXES = [
    "temperatures",
]

"""Hard coded locations of the temperature monitors. The alternative was keeping a record somewhere else
(hard to use SQL statements to select locations). Or reprogramming each unit when moving it (not ideal)"""
LOCATIONS = {
	'temp1': "",
	'temp2': "Study room at sitting level",
	'temp3': "Dining room at floor level",
    'temp4': "Terrences room on phone charging table",
	'temp5': "",
	'temp6': "Study room at floor level",
}


def init():
    """This is run once the database is connected. Do anything that needs to be done only once per script execution here."""
    for create_table in CREATE_TABLES:
        db_execute(create_table)


def temp_update(dt: datetime, host_name: str, temperature: float):
    if host_name in LOCATIONS:
        location = LOCATIONS[host_name]
    else:
        location = "Unknown"
        
    cmd = "INSERT INTO {}(datetime, host_name, temperature, location) VALUES (%s, %s, %s, %s)".format(DATABASE_NAME)
    db_execute(cmd, (dt.strftime("%Y-%m-%d %H-%M-%S.%f"), host_name, temperature, location,))
    

def save_message(msg: Message):
    """This is passed every relevent message that MQTT receives."""
    host_name = msg.topic.split('/')[0]
    temp = float(msg.payload)

    """ This is improper and leads to inaccuracies when starting the client.
    MQTT will resend messages for the last so many minutes if they weren't delivered.
    So if this was stopped for a day, a bunch of out of date messages will come in at the
    same time and be given the same date and time.
    Ideally we would chagne the temperature monitor so the data it sends is a string containing
    the measurement date and time. However, then we are also relying on the datetime on the device.
    We could also change the QOS of the monitor device so the server Doesn't keep stagnent messages
    Good enough for now though."""
    dt = datetime.now()

    temp_update(dt, host_name, temp)
