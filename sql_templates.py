""" SQL for tables that are used in more than one module."""


Hosts = """CREATE TABLE IF NOT EXISTS
hosts(
    last_updated DATETIME NOT NULL,
    name         VARCHAR(128) PRIMARY KEY,
    IP           VARCHAR(15),
    description  VARCHAR(256),
    SSID         VARCHAR(64),
    MAC          VARCHAR(17),
    RSSI         INT)
"""
