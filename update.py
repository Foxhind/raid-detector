#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import gzip
from itertools import chain
from multiprocessing import Pool
import os
import sqlite3
import sys
import urllib
import urllib2
import xml.etree.cElementTree as cElementTree

if sys.version_info[0] < 3:
    reload(sys)
    sys.setdefaultencoding('utf-8') # a hack to support UTF-8 

BASE_URL = 'http://planet.osm.org/replication/changesets/'
FORMAT = '%Y-%m-%dT%H:%M:%SZ'
STATE_URL = BASE_URL + 'state.yaml'
FILE_EXTENSION = '.osm.gz'


class Changeset:
    id = 0
    created_at = 0
    num_changes = 0
    user = ''
    uid = 0
    min_lat = 0
    max_lat = 0
    min_lon = 0
    max_lon = 0
    def __init__(self, dic):
        self.id = int(dic.get('id'))
        self.created_at = int(datetime.datetime.strptime(
                dic.get('created_at'), FORMAT).strftime('%s'))
        self.num_changes = int(dic.get('num_changes'))
        self.user = dic.get('user')
        self.uid = int(dic.get('uid'))
        self.min_lat = float(dic.get('min_lat'))
        self.max_lat = float(dic.get('max_lat'))
        self.min_lon = float(dic.get('min_lon'))
        self.max_lon = float(dic.get('max_lon'))

    def __str__(self):
        return (str(self.id) + ',' + str(self.created_at) + ',' +
                str(self.num_changes) + ',"' + self.user + '",' +
                str(self.uid) + ',' + str(self.min_lat) + ',' +
                str(self.max_lat) + ',' + str(self.min_lon) + ',' +
                str(self.max_lon))

    def toTuple(self):
        return (self.id, self.created_at, self.num_changes, self.user,
                self.uid, self.min_lat, self.max_lat, self.min_lon,
                self.max_lon)

    def get_center(self):
        return ((self.max_lat + self.min_lat) / 2,
                (self.max_lon + self.min_lon) / 2)

    def get_perimeter(self):
        return (abs(self.max_lat - self.min_lat) +
                abs(self.max_lon - self.min_lon)) * 2


def parse_args():
    global args

    path = os.path.dirname(os.path.realpath(__file__))

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='path to database file', default=path + '/detector.sqlite')
    parser.add_argument('-t', '--threads', help='number of download threads', default=2, type=int)
    args = parser.parse_args()
    return args


def load_config(database):
    cursor = database.cursor()
    cursor.executescript("""
        PRAGMA auto_vacuum=FULL;
        CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY ASC, value BLOB);
        CREATE TABLE IF NOT EXISTS changesets(
            id INTEGER PRIMARY KEY ASC,
            created_at INTEGER,
            num_changes INTEGER,
            user TEXT,
            uid INTEGER,
            min_lat REAL,
            max_lat REAL,
            min_lon REAL,
            max_lon REAL
        );
        CREATE INDEX IF NOT EXISTS idx_changesets ON changesets (created_at);
        """)
    cursor.execute('SELECT * FROM config')
    config = cursor.fetchall()

    if len(config) == 0:
        config = {}
    else:
        config = dict(config)

    cursor.close()
    return config


def save_config(data, database):
    data = [i for i in data.iteritems()]
    database.executemany('INSERT OR REPLACE INTO config VALUES (?, ?)', data)
    database.commit()


def save_changesets(changesets, database):
    changesets = map(lambda c: c.toTuple(), changesets)
    database.executemany('INSERT OR REPLACE INTO changesets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', changesets)
    database.commit()


def remove_old_changesets(database):
    database.execute('DELETE FROM changesets WHERE created_at < (SELECT MAX(created_at) FROM changesets) - 172800')
    database.commit()


def download(urls):
    pool = Pool(processes=args.threads)
    # print('Download %s urls' % len(urls))
    changesets = pool.map(download_osc, urls)
    urllib.urlcleanup()
    changesets = list(chain(*changesets))
    return changesets


def download_osc(url):
    # print('Download url %s' % url)
    filename, headers = urllib.urlretrieve(url)
    file = gzip.open(filename, 'r')
    changesets = parse_osc(file)
    # try:
    #     changesets = parse_osc(file)
    # except TypeError:
    #     changesets = []
    #     pass
    # print(changesets)
    return changesets


def get_server_sequence():
    response = urllib2.urlopen(STATE_URL)
    for line in response:
        line = line.split(':')
        if line[0] != 'sequence':
            continue
        try:
            return int(line[1])
        except ValueError:
            return None
    return None


def get_path_from_id(id):
    id = str(id).rjust(9, '0')
    return id[0:3] + '/' + id[3:6] + '/' + id[6:9]


def get_missing_urls(start, end):
    urls = []
    if start != end:
        if start == None:
            start = end
        for id in xrange(start + 1, end + 1):
            urls.append(BASE_URL + get_path_from_id(id) + FILE_EXTENSION)
    return urls


def parse_osc(file):
    dom = cElementTree.parse(file)
    root = dom.getroot()

    changesets = []
    for cs in root.iter('changeset'):
        if (cs.attrib.get('open') != 'false' or
            cs.attrib.get('num_changes') == '0' or
            not 'min_lat' in cs.attrib or
            not 'max_lat' in cs.attrib or
            not 'min_lon' in cs.attrib or
            not 'max_lon' in cs.attrib):
            continue

        changesets.append(Changeset(cs.attrib))
    return changesets


def main():
    parse_args()
    database = sqlite3.connect(args.database)
    config = load_config(database)

    server_sequence = get_server_sequence()
    if not server_sequence:
        sys.exit(1)
    sequence = config.get('server_sequence')

    missing_urls = get_missing_urls(sequence, server_sequence)
    changesets = download(missing_urls)
    save_changesets(changesets, database)
    remove_old_changesets(database)

    config['server_sequence'] = server_sequence
    save_config(config, database)


if __name__ == '__main__':
    sys.exit(main())
