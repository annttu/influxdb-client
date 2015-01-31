#!/usr/bin/env python
# encoding: utf-8

import argparse

from influxdb import InfluxDBClient, client as inf_client
import readline
from datetime import datetime

client = None
readline.parse_and_bind('?: complete')
readline.set_completer_delims(" ")

series = []

tags = ["SELECT", "FROM", "GROUP BY", "COUNT(", "FILL(", "INNER JOIN", "DISTINCT(", "WHERE", "AND", "", "TIME", "VALUE", "ASC", "DESC", "DERIVATIVE(", "COUNT(", "MEAN(", "SUM(", "MIN(", "MAX(","STDDEV(", "FIRST(", "LAST(", "DIFFERENCE(", "MEDIAN(", "MODE(", "LIMIT"]


def influxdb_compete(text, state):
    utext = text.upper()
    current_line = readline.get_line_buffer().strip().upper()
    try:
        if len(current_line) >= readline.get_begidx():
            x = current_line[:readline.get_begidx()].split()
        else:
            x = current_line.split()
    except Exception as e:
        x = []
    if (len(x) > 0 and x[-1] == 'FROM'):
        for t in [i for i in series if i.lower().startswith(text.lower())]:
            if state == 0:
                return t
            else:
                state -= 1
    elif (len(x) > 0 and x[-1] == 'SELECT'):
        for t in [x for x in ['value', 'time'] if x.startswith(text.lower())]:
            if state == 0:
                return "%s " % t
            else:
                state -= 1
    else:
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


def main():
    readline.parse_and_bind("tab: complete")
    readline.add_history("list series")
    readline.set_completer(influxdb_compete)

    load_series()

    while True:
        try:
            command = input("> ")
        except KeyboardInterrupt:
            print("\nbye!")
            break
        except EOFError:
            print("\nbye!")
            break
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



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="InfluxDB command line client")
    parser.add_argument('-u', '--user', help="Username", default="root")
    parser.add_argument('-p', '--password', help="Password", default="root")
    parser.add_argument('-d', '--database', help="Database", default="root")
    parser.add_argument('-s', '--server', help="Server hostname", default="localhost")
    parser.add_argument('--port', help="Port", default=8086, type=int)
    args = parser.parse_args()
    client = InfluxDBClient(args.server, args.port, args.user, args.password, args.database)
    main()
