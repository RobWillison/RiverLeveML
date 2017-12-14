import db_config
import urllib.request
from datetime import datetime, timedelta
import time
import json

secret = 'f0baf96464bbca6b49a36d88f7c2675d'

def allRivers():
    cursor = db_config.cnx.cursor()
    sql = 'SELECT * FROM rivers'
    cursor.execute(sql)
    return cursor.fetchall()

def getJson(riverData, timestamp) :
    latitude = str(riverData['lat'])
    longitude = str(riverData['long'])
    url = "https://api.darksky.net/forecast/"+secret+"/"+latitude +','+longitude+','+str(timestamp)
    print(url)
    webpage = urllib.request.urlopen(url)
    data = json.loads(webpage.read().decode(webpage.info().get_param('charset') or 'utf-8'))
    hourlyData = data['hourly']['data']
    for dataPoint in hourlyData:
        saveData(dataPoint, riverData['id'])

def saveData(data, riverId):
    forecast = 0
    timestamp = data['time']
    precipIntensity = data['precipIntensity']
    precipProbability = data['precipProbability']

    if timestamp <= int(time.mktime(datetime.now().timetuple())):
        forecast = 1

    cursor = db_config.cnx.cursor()
    sql = 'INSERT INTO dark_sky_data (river_id, timestamp, precip_intensicty, precip_probability, json, forecast) VALUES (%s, %s, %s, %s, %s, %s)'
    cursor.execute(sql, (riverId, timestamp, precipIntensity, precipProbability, json.dumps(data), forecast))
    db_config.cnx.commit()

def collectPastDay():
    rivers = allRivers()
    for river in rivers:
        now = datetime.now().replace(hour=0, minute=0)
        start = int(time.mktime(now.timetuple()))
        start -= 86400
        getJson(river, start)


def collectForecastData():
    rivers = allRivers()
    for river in rivers:
        now = datetime.now().replace(hour=0, minute=0)
        now = int(time.mktime(now.timetuple()))
        getJson(river, now)
