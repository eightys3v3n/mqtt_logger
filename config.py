import logging


class Logging:
  file = "log.txt"
  file_level = logging.DEBUG
  file_format = "%(asctime)s %(name)s (%(levelname)s): %(message)s"
  terminal_level = logging.DEBUG
  terminal_format = "%(name)s (%(levelname)s): %(message)s"
  logger_name_justify_length = 15

  logger_specific_log_levels = {
    'Modules.Outlet': logging.DEBUG,
  }
