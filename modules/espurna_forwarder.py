from db_helpers import db_execute
from datetime import datetime
from main import Message
from logging_setup import create_logger
from math import isnan
from mqtt_helpers import publish
import mqtt_helpers as mqtt
import sql_templates


logger = create_logger('Modules.EspurnaForwarder')
CREATE_TABLES = [sql_templates.Hosts]

"""Topics to accept, # means everything."""
ACCEPTED_TOPIC_PREFIXES = [
    "espurna",
]


def init():
    pass


def redirect_temperature(dt, msg):
    host_name = msg.topic.split('/')[1]
    value = float(msg.payload)

    logger.debug(f"Redirecting temperature message from {host_name}")

    if isnan(value):
        logger.warning(f"Received NaN value for {field} from host:{host_name}")
        return

    mqtt.publish(f'temperatures/{host_name}', value)


def save_message(msg: Message):
    """ This is improper and leads to inaccuracies when starting the client.
    MQTT will resend messages for the last so many minutes if they weren't delivered.
    So if this was stopped for a day, a bunch of out of date messages will come in at the
    same time and be given the same date and time.
    Ideally we would chagne the temperature monitor so the data it sends is a string containing
    the measurement date and time. However, then we are also relying on the datetime on the device.
    We could also change the QOS of the monitor device so the server Doesn't keep stagnent messages
    Good enough for now though."""
    dt = datetime.now()

    field = msg.topic.split('/')[-1]

    if field == "temperature":
        redirect_temperature(dt, msg)
    else:
        logger.warn("Received a message that couldn't be saved: {}".format(msg.topic))
