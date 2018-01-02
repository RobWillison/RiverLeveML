import urllib.request
from datetime import datetime, timedelta
import os.path
from Tools import DbPopulationTool
import db_config

def getRainDataImg(dateTime):

    dateString = dateTime.strftime("%Y%m%d%H00")

    url = "http://api.meteoradar.co.uk/image/1.0/?time=" + dateString + "&type=historie#f"
    print(url)
    webpage = urllib.request.urlopen(url)
    data = webpage.read()

    f = open('Data/Rain/' + dateString + '.png', 'wb')
    f.write(data)
    f.close()

    return 'Data/Rain/' + dateString + '.png'

def getRainData(dateTime):
    return getRainDataImg(dateTime)

def checkIfDateHasBeenDone(dateTime):
    dateString = dateTime.strftime("%Y%m%d%H%M")
    return os.path.isfile('Data/Rain/' + dateString + '.png')

def getHour(datetime):
    result = tryToGetImage(datetime)
    if result == None:
        return
    DbPopulationTool.processImages([result])

def getDay(date):
    images = []
    for i in range(24):
        dateToDo = date + timedelta(hours=i)
        result = tryToGetImage(dateToDo)
        if result:
            images.append(result)
    DbPopulationTool.processImages(images)

def tryToGetImage(timeToProcess):
    tries = 0
    while (True):
        try:
            tries += 1
            if(tries > 1):
                print('Given Up On:' + timeToProcess.strftime('%Y-%m-%d %H:%M'))
                return None

            return getRainData(timeToProcess)
        except Exception as ex:
            print(ex)
            print(timeToProcess)

def getLastUpdate():
    cursor = db_config.cnx.cursor()

    sql = "SELECT `time_string` FROM `rain_data` ORDER BY `timestamp` DESC LIMIT 1"
    cursor.execute(sql)

    result = cursor.fetchone()
    return result['time_string']

def doWork():
    setLastRun()
    startDate = getLastUpdate()
    endDate = datetime.now()
    diff = endDate - startDate
    seconds = (diff.days * 86400) + diff.seconds
    for i in range(int(seconds / 3600)):
        day = endDate - timedelta(hours = i + 1)
        print(day)
        getHour(day)

def setLastRun():
    cursor = db_config.cnx.cursor()

    sql = "UPDATE gatherers SET last_run = %s WHERE name = 'Rain'"
    cursor.execute(sql, (datetime.now()))

    db_config.cnx.commit()
