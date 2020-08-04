from db_helpers import db_execute
from main import Message
import logging


logger = logging.getLogger(__name__)
"""Topics to accept, # means everything."""
ACCEPTED_TOPIC_ROOTS = [
    "#",
]
"""Sub-topics to ignore where `root` is the device name or identifier. So this will ignore anything/relay/0/set."""
IGNORE_TOPICS = lambda root:(
    "Microwave/",
    # "PhoneCharger/",
)


def init(database):
    """This is run once the database is connected. Do anything that needs to be done only once per script execution here."""
   

def save_message(msg: Message):
    """This is passed every relevent message that MQTT receives."""
    logger.info(msg)
