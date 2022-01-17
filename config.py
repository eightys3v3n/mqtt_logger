import logging


class General:
  DateTimeFormat = "%Y-%m-%d %H:%M:%S"
  SecretFile = "secret.json" # MQTT host & credentials, SQL credentials.


class Logging:
  file = "logs/log.txt"
  file_level = logging.DEBUG
  file_format = "%(asctime)s %(name)s (%(levelname)s): %(message)s"
  terminal_level = logging.INFO
  terminal_format = "%(name)s (%(levelname)s): %(message)s"
  logger_name_justify_length = 22
  MaxFileSize = 1024 * 1024 * 128 # in bytes
  MaxFiles = 10

  # log levels specified by logger name. only applies to console, not file logging.
  logger_specific_log_levels = {
  }
