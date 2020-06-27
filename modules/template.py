from db_helpers import db_execute


"""Add the file name to the modules/__init__.py file for it to be used."""


"""Commands to create the database, this is run every time the database is opened.
So you need the [IF NOT EXISTS] part. These are some example commands."""
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


"""Topics to accept, # means everything."""
ACCEPTED_TOPIC_PREFIXES = [
    "#",
]
"""Sub-topics to ignore where `root` is the device name or identifier. So this will ignore anything/relay/0/set."""
IGNORE_TOPICS = lambda root:(
    root+"/relay/0/set",
)


def init(database):
    """This is run once the database is connected. Do anything that needs to be done only once per script execution here."""
    for create_table in CREATE_TABLES:
        db_execute(database, create_table)


def host_update(database, host_name: str, column: str, data):
    """An example function showing how to access the database using db_helpers.db_execute()."""
    cmd = "INSERT IGNORE INTO host(name) VALUES(%s)"
    db_execute(database, cmd, (host_name,))
    if column is not None:
        cmd = "UPDATE host SET {}=%s WHERE name=%s".format(column)
        db_execute(database, cmd, (data, host_name))


def save_message(database, msg: main.Message):
    """This is passed every relevent message that MQTT receives."""
    print("recieved message '{}'".format(msg))
