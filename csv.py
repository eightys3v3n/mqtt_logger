import mysql.connector
from pathlib import Path
from datetime import datetime
from progressbar import ProgressBar
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


def get_entries(user, password, host):
    db = mysql.connector.connect(database="mqtt_logger", host="127.0.0.1", user=user, passwd=password)
    cursor = db.cursor()
 
    print("Counting entries...")
    cursor.execute("SELECT COUNT(*) FROM stat WHERE host_name=%s", (host, ))
    total = cursor.fetchone()[0]
    

    print("Retrieving entries into RAM...")
    bar = ProgressBar(max_value=total).start()
    cursor.execute("SELECT {} FROM stat WHERE host_name=%s".format(', '.join(COLUMNS)), (host,))
    entries = []
    
    i = 0
    while True:
        row = cursor.fetchone()
        if row is None: break

        if i % 1000 == 0:
            bar.update(i)

        try:
            entries.append(Entry(row))
        except Exception as e:
            print("Failed to create Entry: {} for row '{}'".format(e, row))
        i += 1
    bar.finish()
    return entries


def csv(file_path, x, y):
    print("Saving...")
    bar = ProgressBar(max_value=len(x)).start()
    with open(file_path, 'w') as f:
        for i, (j, k) in enumerate(zip(x, y)):
            f.write("{},{}\n".format(j.strftime("%Y/%m/%d %H:%M:%S"), k))
            if i % 1000 == 0:
                bar.update(i)
    bar.finish()


def replace_nones(li, to_val):
    li = list(map(lambda x:x if x is not None else to_val, li))
    return li


def main():
    config = json.loads(open('secret.json', 'r').read())
    config = config['sql']

    for host in ("PhoneCharger", "UpstairsFridge"):
        print("Doing host {}".format(host))
        entries = get_entries(*config, host)
    
        dates = list(e.datetime for e in entries)
        powers = list(e.power for e in entries)
        print("Powers with None value: ", powers.count(None))
        powers = replace_nones(powers, 0.0)
        powers = list(map(lambda x:x, powers))

        csv(host+".csv", dates, powers)


if __name__ == '__main__':
    main()
