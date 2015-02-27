#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sqlite3
import sys

if sys.version_info[0] < 3:
    reload(sys)
    sys.setdefaultencoding('utf-8') # a hack to support UTF-8


class GeoJSON:
    data = {'type': 'FeatureCollection'}
    features = []
    def add_point(self, point, name):
        self.features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': (point[1], point[0])
            },
            'properties': {
                'title': name
            }
        })

    def __str__(self):
        self.data['features'] = self.features
        return json.dumps(self.data)


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
    def __init__(self, dic=None, row=None):
        if dic != None:
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
        elif row != None:
            self.id = row[0]
            self.created_at = row[1]
            self.num_changes = row[2]
            self.user = row[3]
            self.uid = row[4]
            self.min_lat = row[5]
            self.max_lat = row[6]
            self.min_lon = row[7]
            self.max_lon = row[8]

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
    parser.add_argument('-d', '--database', help='path to database file', default=path + '/raid-detector.sqlite')
    parser.add_argument('-t', '--threads', help='number of download threads', default=2, type=int)
    args = parser.parse_args()
    return args


def load_changesets(database):
    def changeset_factory(cursor, row):
        return Changeset(row=row)

    database.row_factory = changeset_factory
    cursor = database.cursor()
    cursor.execute('SELECT * FROM changesets')
    changesets = cursor.fetchall()
    cursor.close()
    return changesets


def distance(a, b):
    return ((b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2) ** 0.5


def FOREL(data, radius):
    clusters = []
    while len(data) > 0:
        current = data[0].get_center()
        previous = [0, 0]
        currentcluster = []
        while current != previous:
            previous = current
            nearobjects = []
            masscenter = [0, 0]
            for i in range(len(data)-1, -1, -1):
                center = data[i].get_center()
                if (distance(center, current) <= radius):
                    nearobjects.append(i)
                    masscenter[0] += center[0]
                    masscenter[1] += center[1]
            masscenter[0] /= float(len(nearobjects))
            masscenter[1] /= float(len(nearobjects))
            current = masscenter
        for i in range(len(data)-1, -1, -1):
            if i in nearobjects:
                currentcluster.append(data[i])
                data.pop(i)
        clusters.append(currentcluster)
    return clusters


def FOREL_time(data, radius):
    clusters = []
    while len(data) > 0:
        current = data[0].created_at
        previous = 0
        currentcluster = []
        while current != previous:
            previous = current
            nearobjects = []
            masscenter = 0
            for i in range(len(data)-1, -1, -1):
                if (abs(data[i].created_at - current) <= radius):
                    nearobjects.append(i)
                    masscenter += data[i].created_at
            masscenter /= float(len(nearobjects))
            current = masscenter
        for i in range(len(data)-1, -1, -1):
            if i in nearobjects:
                currentcluster.append(data[i])
                data.pop(i)
        clusters.append(currentcluster)
    return clusters


def main():
    parse_args()
    database = sqlite3.connect(args.database)
    changesets = load_changesets(database)

    geo_clusters = FOREL(changesets, 0.1)
    hour_clusters = []
    day_clusters = []
    for geo_cluster in geo_clusters:
        hour_clusters.extend(FOREL_time(geo_cluster, 1800))
        day_clusters.extend(FOREL_time(geo_cluster, 43200))

    geojson = GeoJSON()
    for cluster in hour_clusters:
        if len(cluster) > 5:
            # users = len(set(map(lambda c: c.user, c[1])))
            geojson.add_point(cluster[0].get_center(), str(len(cluster)))
    print str(geojson)


if __name__ == '__main__':
    sys.exit(main())
