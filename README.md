# MQTT Data Logger

Logs all but a blacklist of topics on an MQTT broker into an SQLite file.

Needs a `secret.json` file containing `[["mqtt_username", "mqtt_password"], ["mqtt_broker_address", port]]`
if the MQTT broker requires authentication.
If there is no authentication leave username and password empty.
