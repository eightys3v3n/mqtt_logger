from mysql.connector import connect
from logging_setup import create_logger


logger = create_logger('Database')


def open_database(user: str, password: str):
    global database

    if database is not None:
        close_database()

    host = "127.0.0.1"
    logger.info("Connecting to SQL database {}@{}".format(user, host))
    database = connect(host=host, user=user, passwd=password, database="mqtt_logger", autocommit=True)


def close_database():
    global database

    logger.info("Closing database connection...")
    try:
        database.commit()
        database.close()
    except NameError: pass
    finally:
        logger.info("Closed database connection.")


def db_execute(cmd, data=None):
    global database

    if database is None:
        logger.error("Attempted to use database without opening it first")
        raise Exception("Database isn't open")

    cursor = database.cursor()
    logger.debug("Executing '{}' with data '{}'".format(cmd, data))
        
    if data is not None:
        cursor.execute(cmd, data)
    else:
        cursor.execute(cmd)
        
    return cursor
