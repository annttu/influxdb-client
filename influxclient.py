
from influxdb import InfluxDBClient, client as inf_client
import readline
from datetime import datetime

client = InfluxDBClient('localhost', 8086, 'root', 'root', 'collectd')

readline.set_completer_delims(" ")

series = []

tags = ["SELECT", "FROM", "GROUP BY", "COUNT(", "FILL(", "INNER JOIN", "DISTINCT(", "WHERE", "AND", "", "TIME", "VALUE", "ASC", "DESC", "DERIVATIVE(", "COUNT(", "MEAN(", "SUM(", "MIN(", "MAX(","STDDEV(", "FIRST(", "LAST(", "DIFFERENCE(", "MEDIAN(", "MODE("]

last_word = None

def influxdb_compete(text, state):
    global last_word
    utext = text.upper()
    x = readline.get_line_buffer().strip().upper().split()
    if x[-1] == 'FROM' or x[-2] == 'FROM':
        for t in [i for i in series if i.lower().startswith(text.lower())]:
            if state == 0:
                return t
            else:
                state -= 1
    else:
        last_word = None
        for t in tags:
            if t.startswith(utext):
                if state == 0:
                    return "%s " % t
                else:
                    state -= 1
    return None

def load_series():
    global series
    res = client.query("list series")
    series = [i[1] for i in res[0]['points']]

readline.parse_and_bind("tab: complete")
readline.add_history("list series")
readline.set_completer(influxdb_compete)

load_series()

while True:
    command = input("> ")
    if not command:
        continue
    cx = command.upper().split()
    cy = command.split()
    if 'FROM' in cx:
        table = cy[cx.index("FROM")+1]
        if not (table.startswith("/") and table.endswith("/")):
            command = ' '.join([' '.join(cy[:cx.index("FROM")+1]), "\"%s\"" % table, ' '.join(cy[cx.index("FROM")+2:])])

    try:
        for i in client.query(command):
            print(i['name'])
            print(' | '.join(i['columns']))
            for point in i['points']:
                pointdata = point[1:]
                print("%s, %s" % (datetime.fromtimestamp(point[0]).strftime("%d.%m.%Y %H:%M:%S"), ', '.join([str(i) for i in pointdata])))
    except inf_client.InfluxDBClientError as e:
        print(e.content.decode("utf-8"))

