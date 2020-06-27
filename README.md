# MQTT Data Logger

Logs MQTT messages into an SQL database using various modules to allow for logging of multiple different devices sending different types of messages.

## Getting started
### Installation
- Requires paho from pip. Please create an issue if I have forgotten dependencies.
- Setup an SQL user and password for this program to access the database through.
- Setup Mosquitto (an MQTT server) with a username and password for this program.
- Run with `python main.py`

### Usage
- Rename `secret.json.example` to `secret.json` and change the MQTT and SQL usernames, passwords, and hosts. If there is no MQTT authentication leave username and password empty.
- This program should print out all the MQTT messages it receives. Use this to make a module that converts the received messages into SQL data and adds it to the appropriate tables. **Modules are not implemented yet. This currently only handles a smart outlet running Espirna and logging to the MQTT server).**

## Modules
Modules are all created by copying the modules/template.py file. They should be detected on next run and all relavent messages passed to them.

### Espurna Smart Outlet
Records into host(name, IP, description, SSID, MAC, RSSI) and stat(datetime, host_name, state, current, voltage, power, energy) table. Set the outlet to QOS 2 to not break the datetime rounding. Set the root topic to {hostname}, this will be used as the host.name and stat.host_name. Set "Power Units" to watts and "Energy Units" to killowatt hours.  
- stat.datetime has some odd rounding such that all messages received in a given interval are set to the same datetime. This allows for easy searching. Without this there is no way to put all the collected information into a single row. Each piece of information comes in at a different time.
- stat. everything else is as shown on the Espirna pages. Power is current Wattage, Energy is recorded Watt hours.
### Temperature recorder
This is to-be-implemented for temperature monitors with code yet-to-be written. It will allow for recording of temperature data from multiple devices in the same way the Espurna Smart Outlet module does for power usage.
