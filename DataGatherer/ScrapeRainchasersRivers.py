import urllib.request
from bs4 import BeautifulSoup
import json
import re
import db_config

def swapUrls():
    cursor = db_config.cnx.cursor()
    sql = 'SELECT id, station_url FROM rivers WHERE source_agency = "ea" AND station_url IS NOT NULL'
    cursor.execute(sql)

    results = cursor.fetchall()
    for result in results:
        station_id = result['station_url'].split('=')[-1]
        url = 'https://flood-warning-information.service.gov.uk/station/' + station_id
        print(url)

        sql = 'UPDATE rivers SET station_url = %s Where station_url = %s'
        cursor.execute(sql, (url, result['station_url']))
        db_config.cnx.commit()

def gotUuid(uuid):
    cursor = db_config.cnx.cursor()
    sql = 'SELECT id FROM rivers WHERE uuid = %s'
    cursor.execute(sql, (uuid))

    result = cursor.fetchone()
    return result != None


def getRiversJson(url = None):
    if url == None:
        url = 'http://api.rainchasers.com/v1/river'



    webpage = urllib.request.urlopen(url)
    html = webpage.read().decode('utf-8')
    print(url)
    jsonArray = json.loads(html)

    for river in jsonArray['data']:
        latitude = river['putin']['lat']
        longitude = river['putin']['lng']
        riverName = river['river']
        riverSection = river['section']
        uuid = river['uuid']
        if gotUuid(uuid):
            continue
        calibrationString, source, sourceUrl, station = getRiverInfoFromJs(uuid)

        writeToDb(riverName, riverSection, longitude, latitude, calibrationString, source, station, sourceUrl, uuid)

    nextUrl = jsonArray['meta']['link']['next']
    print(nextUrl)
    if nextUrl:
        getRiversJson(nextUrl)

def getRiverInfoFromJs(uuid):
    url = 'http://api.rainchasers.com/v1/river/' + uuid + '/level.js'
    print(url)
    webpage = urllib.request.urlopen(url)
    html = webpage.read().decode('utf-8')

    jsonString = html.split(u'o.data=')[1]
    jsonString = jsonString.split(u';return')[0]

    riverJson = json.loads(jsonString)

    calibration = riverJson['calibration']
    source = riverJson['source']

    if source != None:
        return str(calibration), source['type'], source['url'], source['name']

    return str(calibration), None, None, None

def writeToDb(riverName, sectionName, long, lat, calibrationString, source, station, url, uuid):
    cursor = db_config.cnx.cursor()

    sql = "REPLACE INTO rivers (river, section, lat, `long`, level_indicators, source_agency, station, station_url, uuid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, (riverName, sectionName, float(lat), float(long), calibrationString, source, station, url, uuid))

    db_config.cnx.commit()
