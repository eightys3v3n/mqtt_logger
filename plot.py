from sqlite3 import connect
from bokeh.plotting import output_file, figure, show
from bokeh.models import DatetimeTickFormatter
from pathlib import Path
from datetime import datetime


def get_entries(path):
    db = connect(path)
    res = db.execute("SELECT datetime, power FROM power")
    entries = res.fetchall()
    return entries


def plot(x, y):
    output_file("/tmp/plot.html", mode="inline")
    p = figure(title="Power Usage",
               x_axis_label="Date/Time",
               y_axis_label="Power (W)",
               x_axis_type='datetime',
               sizing_mode="stretch_both")
    p.line(x, y, line_width=2)
    return p


def main():
    #path = Path(input("Database path: "))
    path = Path("database.sqlite")
    if not path.exists():
        raise ValueError("File doesn't exist '{}'".format(path))
    entries = get_entries(path)
    print("{} entries to plot".format(len(entries)))
    dates = list(datetime.strptime(e[0], "%Y-%m-%d %H:%M:%S.%f") for e in entries)
    powers = list(e[1]*1000 for e in entries)

    p = plot(dates, powers)
    show(p)


if __name__ == '__main__':
    main()
