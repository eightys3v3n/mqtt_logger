# MQTT Data Logger

Logs MQTT messages into an SQL database using various modules to allow for logging of multiple different devices sending different types of messages.

## Getting started
### Installation
- Requires paho from pip.
- Setup an SQL user and password for this program to access the database through.
- Setup Mosquitto (an MQTT server) with a username and password for this program.
- Rename `secret.json.example` to `secret.json` and change the MQTT and SQL usernames, passwords, and hosts. If there is no MQTT authentication leave username and password empty.
- Run with `python main.py`

## Modules
Modules are loaded by the program and are handed what every MQTT messages they claim to be able to deal with.

### Espurna Devices Info
Records device information from MQTT messages formatted as `espurna/<device_name>/info`.
Stored into an SQL table whose table creation syntax is in sql_templates.py.
Currently supports the following fields reported by Espurna:
- ip :: str IP address
- desc :: str Description
- ssid :: str WiFi Name
- mac :: str MAC address
- rssi :: int WiFi signal strength

### Espurna Forwarder
Receives temperature messages in `espurna/<device_name>/temperature` and reposts them to `temperatures/<device_name>`.

### Name-Host-Value
Receives messages from root topics containing device IDs and saves them into their respective SQL tables.
Currently supports the following SQL tables and MQTT topics
- temperatures
- relative_humidities
- total_volatile_organic_compounds

### Espurna Smart Outlet
Records power usage stats from Sonoff S31 outlets running Espurna.
Currently supports the following fields:
- state :: whether the outlet is on(1) or off(0)
- current :: amps
- voltage
- power :: watts
- energy :: kWh
Datetime undergoes some rounding such that all messages received in a given interval are set to the same datetime. This allows for easy searching.
Without this there is no way to put all the collected information into a single row. Each piece of information comes in at a different time.
