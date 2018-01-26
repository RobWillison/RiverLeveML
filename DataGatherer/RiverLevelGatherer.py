import csv
import urllib.request
import gzip
from datetime import datetime, timedelta
import time
import db_config
from bs4 import BeautifulSoup

def saveRiverData(dateTime, stations):
    print(dateTime.strftime('%Y-%m-%d'))
    url = "http://environment.data.gov.uk/flood-monitoring/archive/readings-full-" + dateTime.strftime('%Y-%m-%d') + ".csv"
    print(url)
    webpage = urllib.request.urlopen(url)

    responseCompressed = webpage.read()

    responseDecompressed = gzip.decompress(responseCompressed).decode("utf-8", "strict").splitlines()

    csvReader = csv.reader(responseDecompressed)

    levelData = []
    firstRow = True

    nameId = 0
    timestampId = 0
    levelId = 0

    for row in csvReader:
        if (firstRow):
            print(row)
            nameId = row.index('label')
            timestampId = row.index('dateTime')
            levelId = row.index('value')
            firstRow = False
            continue

        name = row[nameId]

        station = [x for x in stations if x[1] == name]

        if len(station) < 1:
            continue

        station = station[0]

        timestamp = row[timestampId]

        level = row[levelId]

        dateTaken = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')

        saveData(name, dateTaken, level, station[0])


def saveData(name, dateTime, riverLevel, riverId):
    time_string = dateTime.strftime("%Y-%m-%d %H:%M")
    timestamp = int(time.mktime(dateTime.timetuple()) + 3600) #make it UTC for some reason it is -1 hour out

    cursor = db_config.cnx.cursor()

    sql = "REPLACE INTO `river_data` (`timestamp`, `station`, `time_string`, `river_level`, `river_id`) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(sql, (timestamp, name, time_string, riverLevel, riverId))

    db_config.cnx.commit()

def checkIfDateHasBeenDone(dateTime):
    timestamp = int(time.mktime(dateTime.timetuple()) + 3600) #make it UTC for some reason it is -1 hour out

    cursor = db_config.cnx.cursor()

    sql = "SELECT * FROM `river_data` WHERE `timestamp` = %s"
    cursor.execute(sql, (timestamp))

    result = cursor.fetchone()

    db_config.cnx.commit()

    return result != None

def checkForUpdates():
    cursor = db_config.cnx.cursor()
    sql = "SELECT id, station, station_url FROM rivers WHERE station IS NOT NULL"
    cursor.execute(sql)
    stations = list(map(lambda x: [x['id'], x['station'], x['station_url']], cursor.fetchall()))

    sql = "SELECT `time_string` FROM `river_data` ORDER BY `timestamp` DESC LIMIT 1"
    cursor.execute(sql)

    result = cursor.fetchone()
    dateNow = datetime.now()
    lastDate = result

    if result != None:
        lastDate = getLastRun()

    print(lastDate)
    diff = dateNow - lastDate
    if diff.days == 0:
        getRiverLevelsNow(stations)
        return

    for i in range(diff.days):
        dateToDo = dateNow - timedelta(days=i + 1)
        print(dateToDo)
        tryAndSaveRiverData(dateToDo, stations)

    setLastRun()



def getRiverLevelsNow(stations):
    for station in stations:
        if station[2] == None:
            continue
        print(station)
        webpage = urllib.request.urlopen(station[2])
        html = webpage.read().decode('utf-8')
        soup = BeautifulSoup(html)
        table = soup.find("table", id="telemetry")
        if table == None:
            continue

        rows = table.find_all('tr')
        river_levels = []
        for row in rows:
            columns = row.find_all('td')
            if not columns:
                continue

            time = columns[0].text
            value = columns[1].text
            if value == '':
                value = '0'

            saveData(station[1], datetime.strptime(time, '%Y-%m-%dT%H:%MZ'), value, station[0])




def tryAndSaveRiverData(timeToProcess, stations):
    tries = 0
    while (True):
        try:
            tries += 1
            if(tries > 5):
                print('Given Up On:' + timeToProcess.strftime('%Y-%m-%d %H:%M'))
                return

            saveRiverData(timeToProcess, stations)
            return
        except Exception as ex:
            print(ex)
            continue

def setLastRun():
    cursor = db_config.cnx.cursor()

    sql = "UPDATE gatherers SET last_run = %s WHERE name = 'River'"
    cursor.execute(sql, (datetime.now()))

    db_config.cnx.commit()

def getLastRun():
    cursor = db_config.cnx.cursor()

    sql = "SELECT * FROM gatherers WHERE name = 'River'"
    cursor.execute(sql)
    result = cursor.fetchone()
    return result['last_run']
