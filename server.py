import tornado.ioloop
import tornado.web
import tornado.escape
import collections
import json

import sqlite3 as lite
import sys

import logging

from ini_logger import *

log_dir = r'/home/pi/Documents/beat2020software/Server/logs'#'/var/log/server'
ini_logger(log_dir)
#sudo nano /etc/rc.local

# Test with:
# sqlite3 /home/pi/Documents/beat2020software/Server/sensor.db
# SELECT* FROM data;

def create_tables(_connector):
    cursor = _connector.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS devices (id UNSIGNED INT KEY, name TEXT)")
    cursor.execute("""CREATE TABLE IF NOT EXISTS data (
device UNSIGNED INT,
messure_time UNSIGNED MEDIUMINT,
bpm UNSIGNED SMALLINT,
p_set FLOAT,
pressure FLOAT,
flow FLOAT,
volume FLOAT,
recive_time TIMESTAMP
)""")


def get_all_tables(_connector):
    sql = '''SELECT name FROM sqlite_master WHERE type ='table' '''
    cursor = _connector.cursor()
    cursor.execute(sql)
    datas = cursor.fetchall()
    connector.commit()
    ret = []
    for data in datas:
        ret.append(data[0])
    return ret


def add_device(_connector, _device):
    cursor = _connector.cursor()
    sql = "SELECT * FROM devices WHERE id=?"
    cursor.execute(sql, (_device[0],))
    if not cursor.fetchall():
        sql = "INSERT INTO devices (id, name) VALUES (?, ?)"
        cursor.execute(sql, _device)
        logging.info("[SQL]: add device with id = {} and name= {}".format(_device[0], _device[1]))
    connector.commit()


def update_deviceName(_connector, _device):
    cursor = connector.cursor()
    sql = "SELECT * FROM devices WHERE id=?"
    cursor.execute(sql, (_device[0],))
    if cursor.fetchall():
        sql = '''UPDATE devices
                 SET name = ?
                 WHERE id = ?'''
        cursor.execute(sql, (_device[0], _device[1]))
        logging.info("[SQL]: update device with id = {} and new name= {}".format(_device[0], _device[1]))
    _connector.commit()


def add_data(_connector, data):
    sql = "INSERT INTO data (device, messure_time, bpm, p_set, pressure, flow, volume, recive_time) VALUES (?, ?, ?, ?, ?, ?, ?, DATETIME('now'))"
    cursor = _connector.cursor()
    cursor.execute(sql, data)
    _connector.commit()
    logging.info("[SQL]: add data from device = {}".format(data[0]))


def get_all_data(_connector, table):
    sql = "SELECT * FROM '%s'"
    cursor = _connector.cursor()
    cursor.execute(sql % table)
    datas = cursor.fetchall()
    _connector.commit()
    ret = []
    for data in datas:
        sub_ret = []
        for i in range(len(data)):
            sub_ret.append(data[i])
        ret.append(sub_ret);
    return ret


def clear_data(_connector):
    sql = '''DELETE FROM data
            WHERE recive_time <= DATETIME('now','-10 second')'''  # -10 minute -4 hour
    cursor = _connector.cursor()
    cursor.execute(sql)
    logging.info("[SQL]: cleared old data")
    _connector.commit()


def get_data_as_json(_connector, _device_id):
    cursor = _connector.cursor()
    sql = "SELECT * FROM devices WHERE id = ?"
    cursor.execute(sql, (_device_id,))
    names = cursor.fetchall()
    _connector.commit()
    cursor = _connector.cursor()
    sql = "SELECT * FROM data WHERE device = ?"
    cursor.execute(sql, (_device_id,))
    datas = cursor.fetchall()
    _connector.commit()
    ret = {}
    ret['name'] = names[0][1]
    ret['data'] = []
    for data in datas:
        sub_ret = {}
        sub_ret['time'] = data[1]
        sub_ret['bpm'] = data[2]
        sub_ret['p_set'] = data[3]
        sub_ret['pressure'] = data[4]
        sub_ret['flow'] = data[5]
        sub_ret['volume'] = data[6]
        ret['data'].append(sub_ret)
    logging.info("[SQL]: select data from device = {}".format(_device_id))
    return ret


def get_devices_as_json(_connector):
    sql = "SELECT * FROM devices"
    cursor = connector.cursor()
    cursor.execute(sql)
    datas = cursor.fetchall()
    connector.commit()
    ret = {}
    ret['devices'] = []
    for data in datas:
        sub_ret = {}
        sub_ret['id'] = data[0]
        sub_ret['name'] = data[1]
        ret['devices'].append(sub_ret)
    logging.info("[SQL]: select all devices")
    return ret


class DeviceHandler(tornado.web.RequestHandler):
    def initialize(self, database):
        self.set_header("Content-Type", "application/json")
        self.connector = lite.connect(database)

    def prepare(self):
        if self.request.headers.get('Content-Type') == 'application/json':
            print(self.request.body)
            self.json_args = json.loads(self.request.body.decode('utf-8'))
        else:
            self.json_args = None

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def get(self, param):
        logging.debug("[RequestHandler]: GET with param = " + param)
        if param.isdigit():
            device_id = param
            with self.connector:
                response = get_data_as_json(connector, device_id)
            self.write(json.dumps(response))
        elif param == "all":
            with self.connector:
                response = get_devices_as_json(connector)
            self.write(json.dumps(response))
        else:
            raise tornado.web.HTTPError(404)

    def post(self, param):
        logging.debug("[RequestHandler]: POST with param = " + param)
        if param.isdigit():
            logging.info("[RequestHandler]: POST data from device: " + param)
            device_id = param
            with self.connector:
                device = (device_id, 'TestDevice' + str(device_id))
                add_device(self.connector, device)
            if self.json_args is not None:
                jdatas = self.json_args["data"]
                for jdata in jdatas:
                    data = (device_id, jdata["time"], jdata["bpm"], jdata["p_set"], jdata["p_c"], jdata["flow_c"],
                            jdata["vol_c"])
                    with self.connector:
                        add_data(self.connector, data)
                with self.connector:
                    clear_data(self.connector)
        elif param == "update":
            if self.json_args is not None:
                devices = self.json_args["devices"]
                for jdevice in devices:
                    device = (jdevice["id"], jdevice["name"])
                    with self.connector:
                        update_deviceName(self.connector, device)
                with self.connector:
                    clear_data(self.connector)
        else:
            raise tornado.web.HTTPError(404)
        logging.info(
            "[RequestHandler]: POST processed request with param: {} and body: {}".format(param, self.request.body))


class LoggerHandler(tornado.web.RequestHandler):
    def get(self):
        with open(log_dir + '/server_all.log') as f:
            f = f.readlines()
        for line in f:
            self.write(line + "<br>")


class DataBaseHandler(tornado.web.RequestHandler):
    def initialize(self, database):
        self.connector = lite.connect(database)
        self.tables = get_all_tables(self.connector)

    def get(self, param):
        if param == "all":
            self.tables = get_all_tables(self.connector)
            for table in self.tables:
                self.write(table)
                self.write("<br>")
        elif param in self.tables:
            datas = get_all_data(self.connector, param)
            self.write(r'<table style="width:50%">')
            for data in datas:
                self.write(r'<tr>')
                for val in data:
                    self.write(r'<td>' + str(val) + r'</td>')
                self.write(r'</tr>')
            self.write(r'</table>')
        else:
            raise tornado.web.HTTPError(404)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("/db/<br>")
        self.write("/db/all<br>")
        self.write("/db/<table><br>")
        self.write("<br>")
        self.write("/device/<br>")
        self.write("/device/all<br>")
        self.write("/device/<deviceID><br>")
        self.write("/device/update<br>")
        self.write("<br>")
        self.write("/logs<br>")


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/device/(.*)", DeviceHandler, dict(database='sensor.db')),
        (r"/logs", LoggerHandler),
        (r"/db/(.*)", DataBaseHandler, dict(database='sensor.db')),
    ])


if __name__ == "__main__":
    connector = lite.connect(r'/home/pi/Documents/beat2020software/Server/sensor.db')
    with connector:
        create_tables(connector)

    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
