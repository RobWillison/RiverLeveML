import db_config
import urllib.request
from datetime import datetime, timedelta
import time
from Tools import ImageTools
from Tools import DbPopulationTool
from bs4 import BeautifulSoup


def getRainDataImgMeteo(dateTime):

    dateString = dateTime.strftime("%Y%m%d%H%M")

    url = "http://api.meteoradar.co.uk/image/1.0/?time=" + dateString + "&type=langetermijnregen#f"
    print(url)
    webpage = urllib.request.urlopen(url)
    data = webpage.read()

    f = open('Data/RainForecast/' + dateString + '.jpg', 'wb')
    f.write(data)
    f.close()

    return 'Data/RainForecast/' + dateString + '.jpg'

def getRainDataImgWeatherOnline(dateTime):
    dateStringSlashed = dateTime.strftime("%Y/%m/%d")
    dateString = dateTime.strftime("%Y%m%d")
    hour = dateTime.strftime("%H")

    url = "http://www.images-weatheronline.com/daten/vorher/500px/" + dateStringSlashed + "/n/ukuk/n_" + dateString + "_h" + hour + "_ukuk_en.gif"
    print(url)
    headers = { 'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36' }
    req = urllib.request.Request(url, data=None, headers=headers)
    webpage = urllib.request.urlopen(req)
    data = webpage.read()

    f = open('Data/RainForecast/' + dateTime.strftime("%Y%m%d%H%M") + '.gif', 'wb')
    f.write(data)
    f.close()

    return 'Data/RainForecast/' + dateTime.strftime("%Y%m%d%H%M") + '.gif'


def tryToGetImage(timeToProcess, source, blockSize):
    tries = 0
    while (True):
        try:
            tries += 1
            if(tries > 5):
                print('Given Up On:' + timeToProcess.strftime('%Y-%m-%d %H:%M'))
                return None
            if source == 'Meteo':
                fileName = getRainDataImgMeteo(timeToProcess)
            elif source == 'WeatherOnline':
                fileName = getRainDataImgWeatherOnline(timeToProcess)

            return ImageTools.processForecastImage(fileName, {'blockSize' : blockSize, 'type' : source})
        except Exception as ex:
            print(ex)
            print(timeToProcess)


def writeAreaValuesToForecastTable(areaValues, dateTime, source):
    timeString = dateTime.strftime("%Y-%m-%d %H:%M")
    timestamp = time.mktime(dateTime.timetuple())

    cursor = db_config.cnx.cursor()
    areaId = 0
    for y in range(len(areaValues)):
        for x in range(len(areaValues[y])):
            sql = "REPLACE INTO `rain_forecast_data` (`timestamp`, `time_string`, `rain_value`, `area_id`, `source`) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (timestamp, timeString, areaValues[y][x], areaId, source))

            areaId += 1
            db_config.cnx.commit()

def writeRowToForecastTable(dateTime, rain_value, area_id, source):
    timeString = dateTime.strftime("%Y-%m-%d %H:%M")
    timestamp = time.mktime(dateTime.timetuple())
    cursor = db_config.cnx.cursor()

    sql = "REPLACE INTO `rain_forecast_data` (`timestamp`, `time_string`, `rain_value`, `area_id`, `source`) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(sql, (timestamp, timeString, rain_value, area_id, source))

    db_config.cnx.commit()

def getClimendo(river_id):
    place = getPlaceName(river_id)
    now = datetime.now().replace(second=0)
    url = 'http://climendo.com/en/weather/hourly/united-kingdom/' + place
    webpage = urllib.request.urlopen(url)
    html = webpage.read()
    parsed_html = BeautifulSoup(html)

    container = parsed_html.body.find('div', attrs={'class':'hourly'})
    hours = container.find_all('div')
    for hour in hours:
        if 'hourly-row' in hour['class']:
            time = hour.find('span', attrs={'class':'time'}).text
            now = now.replace(hour=int(time.split(':')[0]), minute=int(time.split(':')[1]))
            rain = hour.find('span', attrs={'class':'rain'}).text
            print(rain + ':' + str(now))
            writeRowToForecastTable(now, float(rain[:-2]) * 100, 811, 2)
        if 'header' in hour['class']:
            if hour.text == 'Tomorrow':
                now = now + timedelta(days=1)

def getPlaceName(river_id):
    if river_id == 1:
        return 'buckfastleigh-2654416'
    raise Error()

def doWork():
    setLastRun()
    # getClimendo(1)
    date = datetime.now()
    images = []
    date = date.replace(hour=0, minute=0)
    for i in range(8 * 7):
        dateToDo = date + timedelta(hours= (i + 1) * 3)
        result = tryToGetImage(dateToDo, 'Meteo', 50)
        if result:
            writeAreaValuesToForecastTable(result, dateToDo, 1)
        # result = tryToGetImage(dateToDo, 'WeatherOnline', 25)
        # if result:
        #     writeAreaValuesToForecastTable(result, dateToDo, 3)

def setLastRun():
    cursor = db_config.cnx.cursor()

    sql = "UPDATE gatherers SET last_run = %s WHERE name = 'Rain Forecast'"
    cursor.execute(sql, (datetime.now()))

    db_config.cnx.commit()
