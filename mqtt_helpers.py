from paho.mqtt.client import Client
from logging_setup import create_logger


logger = create_logger('Database')
client = None


def create(client_id=None, on_connect=None, on_message=None):
  global client

  client = Client(client_id=client_id)
  client.on_connect = on_connect
  client.on_message = on_message


def connect(host, port):
  client.connect(host, port, 60)


def username_pw_set(username, password):
  client.username_pw_set(username, password)


def loop_start():
  client.loop_start()


def disconnect():
  client.disconnect()
