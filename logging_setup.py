import logging
import config


def create_logger(name):
  justified_name = name.ljust(config.Logging.logger_name_justify_length)
  logger = logging.getLogger(justified_name)

  # Set the level of the entire logger so each handler can correctly set their level.
  logger.setLevel(logging.DEBUG)

  # Console logger
  c_handler = logging.StreamHandler()
  c_handler.setLevel(config.Logging.terminal_level)
  if name in config.Logging.logger_specific_log_levels:
    c_handler.setLevel(config.Logging.logger_specific_log_levels[name])
  c_format = logging.Formatter(config.Logging.terminal_format)
  c_handler.setFormatter(c_format)


  # File logger
  f_handler = logging.FileHandler(config.Logging.file)
  f_handler.setLevel(config.Logging.terminal_level)
  f_format = logging.Formatter(config.Logging.file_format)
  f_handler.setFormatter(f_format)


  logger.addHandler(c_handler)
  logger.addHandler(f_handler)

  return logger
