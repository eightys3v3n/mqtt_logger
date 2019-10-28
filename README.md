# MQTT Data Logger

Logs all but a blacklist of topics on an MQTT broker into an SQLite file.

Needs a `secret.json` file containing `[["mqtt_username", "mqtt_password"], ["mqtt_broker_address", port]]`
if the MQTT broker requires authentication.
If there is no authentication leave username and password empty.


## Todo
- Switch over to MySQL from SQLite
- Collect all readings made in a particular time period into a single row. The way it is currently recorded it is hard to get all the data points from a device at a particular time.
