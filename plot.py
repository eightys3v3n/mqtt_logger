import mysql.connector
from bokeh.plotting import output_file, figure, show
from bokeh.models import DatetimeTickFormatter
from pathlib import Path
from datetime import datetime
import json


COLUMNS = ["datetime", "state", "current", "voltage", "power", "energy"]

class Entry:
    def __init__(self, row):
        assert len(row) == len(COLUMNS)
        self.datetime = row[COLUMNS.index("datetime")]
        assert isinstance(self.datetime, datetime), self.datetime

        self.state = row[COLUMNS.index("state")]
        self.current = row[COLUMNS.index("current")]
        self.voltage = row[COLUMNS.index("voltage")]
        self.power = row[COLUMNS.index("power")]
        self.energy = row[COLUMNS.index("energy")]


    def __str__(self):
        return self.__dict__.__str__()


def get_entries(user, password, host="PhoneCharger"):
    db = mysql.connector.connect(database="mqtt_logger", host="127.0.0.1", user=user, passwd=password)
    cursor = db.cursor()
    cursor.execute("SELECT {} FROM stat WHERE host_name=%s".format(', '.join(COLUMNS)), (host,))
    entries = []
    for i, row in enumerate(cursor.fetchall()):
        try:
            entries.append(Entry(row))
        except Exception as e:
            print("Failed to create Entry: {} for row '{}'".format(e, row))
    return entries


def plot(x, y):
    output_file("plot.html", mode="inline")
    p = figure(title="Power Usage",
               x_axis_label="Date/Time",
               y_axis_label="Power (W)",
               x_axis_type='datetime',
               sizing_mode="stretch_both")
    p.line(x, y, line_width=2)
    return p


def replace_nones(li, to_val):
    li = list(map(lambda x:x if x is not None else to_val, li))
    return li


def main():
    config = json.loads(open('secret.json', 'r').read())
    config = config['sql']

    entries = get_entries(*config)
    print("{} entries to plot".format(len(entries)))
    
    dates = list(e.datetime for e in entries)
    powers = list(e.power for e in entries)
    print(powers.count(None))
    powers = replace_nones(powers, 0.0)
    powers = list(map(lambda x:x*1000, powers))

    p = plot(dates, powers)
    show(p)


if __name__ == '__main__':
    main()
