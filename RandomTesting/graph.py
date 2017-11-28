import plotly.plotly as py
import plotly.graph_objs as go
import datetime
import time
import pymysql.cursors
import numpy
import numpy as np
from sklearn import linear_model
import pickle

dbconfig = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'db': 'rainData',
    'port': 3306,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}
regr = pickle.load(open( "classifier.clf", "rb" ))

def getRiverLevelData(timestamp):
    cnx = pymysql.connect(**dbconfig)

    cursor = cnx.cursor()
    sqlBefore = "SELECT * FROM rainData.river_data WHERE `timestamp` < %s ORDER BY `timestamp` DESC LIMIT 1"
    sqlAfter = "SELECT * FROM rainData.river_data WHERE `timestamp` > %s ORDER BY `timestamp` ASC LIMIT 1"

    cursor.execute(sqlBefore, (timestamp))
    resultBefore = cursor.fetchone()

    cursor.execute(sqlAfter, (timestamp))
    resultAfter = cursor.fetchone()

    if resultAfter == None:
        return resultBefore['river_level']

    interpolatedValue = resultBefore['river_level'] + \
        ((resultBefore['river_level'] - resultAfter['river_level']) / (resultBefore['timestamp'] - resultAfter['timestamp'])) * \
        (timestamp - resultBefore['timestamp'])

    cnx.close()
    return interpolatedValue

def getRainLevelDataForTime(timestamp):
    timestamp -= 3600 * 9
    cnx = pymysql.connect(**dbconfig)

    cursor = cnx.cursor()
    sqlBefore = "SELECT * FROM rainData.rain_data WHERE `timestamp` < %s AND area_id = 811 ORDER BY `timestamp` DESC LIMIT 1"
    sqlAfter = "SELECT * FROM rainData.rain_data WHERE `timestamp` > %s AND area_id = 811 ORDER BY `timestamp` ASC LIMIT 1"

    cursor.execute(sqlBefore, (timestamp))
    resultBefore = cursor.fetchone()

    cursor.execute(sqlAfter, (timestamp))
    resultAfter = cursor.fetchone()

    if resultAfter == None:
        return resultBefore['rain_value']

    if resultBefore == None:
        return resultAfter['rain_value']

    interpolatedValue = resultBefore['rain_value'] + \
        ((resultBefore['rain_value'] - resultAfter['rain_value']) / (resultBefore['timestamp'] - resultAfter['timestamp'])) * \
        (timestamp - resultBefore['timestamp'])

    cnx.close()
    return interpolatedValue

def getRainLevelDataForTimeForecast(timestamp):
    timestamp -= 3600 * 9
    cnx = pymysql.connect(**dbconfig)

    cursor = cnx.cursor()
    sqlBefore = "SELECT * FROM rainData.rain_data WHERE `timestamp` < %s AND area_id = 811 ORDER BY `timestamp` DESC LIMIT 1"
    sqlAfter = "SELECT * FROM rainData.rain_data WHERE `timestamp` > %s AND area_id = 811 ORDER BY `timestamp` ASC LIMIT 1"

    cursor.execute(sqlBefore, (timestamp))
    resultBefore = cursor.fetchone()

    cursor.execute(sqlAfter, (timestamp))
    resultAfter = cursor.fetchone()

    if resultAfter == None:
        return resultBefore['rain_value']

    if resultBefore == None:
        return resultAfter['rain_value']

    interpolatedValue = resultBefore['rain_value'] + \
        ((resultBefore['rain_value'] - resultAfter['rain_value']) / (resultBefore['timestamp'] - resultAfter['timestamp'])) * \
        (timestamp - resultBefore['timestamp'])

    cnx.close()
    return interpolatedValue

def getRainLevelData(start, end):
    start -= 3600 * 9
    end -= 3600 * 9

    total = 0
    rainValue = 0
    while start < end:
        rainValue += getRainLevelDataForTime(start) / 10.0
        start += 3600
        total += 1

    return rainValue / total

def getRainLevelDataForecast(start, end):
    start -= 3600 * 9
    end -= 3600 * 9

    total = 0
    rainValue = 0
    while start < end:
        rainValue += getRainLevelDataForTimeForecast(start) / 10.0
        start += 3600
        total += 1

    return rainValue / total


# Tminus1P – Precipitation on previous day
# Tminus1Q – Discharge on previous day
# Tminus2P – Precipitation two days ago
# Tminus2Q – Discharge two days ago
# weekBeforeP – Total precipitation the week before Tminus2
# weekBeforeQ – Average discharge on the week before Tminus2
# weekBeforeBeforeP – etc.
# weekBeforeBeforeQ
# monthBeforeP
# monthBeforeQ
# monthBeforeBeforeP
# monthBeforeBeforeQ
# fourMonthsBeforeP
# fourMonthsBeforeQ
# fourMonthsBeforeBeforeP

def getFeatureForTime(timestamp):
    PricipNow = getRainLevelDataForTimeForecast(timestamp)
    PrecipLastHour = getRainLevelDataForecast(timestamp - 3600, timestamp)
    PrecipLast5Hour = getRainLevelDataForecast(timestamp - (3600 * 5), timestamp)
    Precip1day = getRainLevelDataForecast(timestamp - 86400, timestamp)
    RiverLevel1HourAgo = None # getRiverLevelData(timestamp - 3600)
    RiverLevel1dayAgo = None #getRiverLevelData(timestamp - 86400)

    Precip2dayAgo = getRainLevelDataForecast(timestamp - 86400 - 86400 - 86400, timestamp - 86400 - 86400)
    RiverLevel2dayAgo = None #getRiverLevelData(timestamp - 86400 - 86400)
    PrecipWeek = getRainLevelDataForecast(timestamp - 604800, timestamp)

    actual = None # getRiverLevelData(timestamp)

    return [
    PricipNow,
    PrecipLastHour,
    PrecipLast5Hour,
    Precip1day,
    RiverLevel1HourAgo,
    RiverLevel1dayAgo,
    RiverLevel2dayAgo,
    PrecipWeek,
    ], actual


def predictNextStep(previousRiverLevels, time):

    feature, actual = getFeatureForTime(time)

    feature[4] = previousRiverLevels[-1]
    feature[5] = previousRiverLevels[-24]
    feature[6] = previousRiverLevels[-48]

    predictedRiverLevel = regr.predict([feature])

    return predictedRiverLevel, actual

def predictNext7days(riverLevels, rainLevels, timestamps, start):
    step = 3600

    actualLevels = []
    for i in range(100):
        timestamps.append(start + step * i)
        rainLevels.append(getRainLevelDataForTimeForecast(start + step * i) / 4000)
        predictedRiverLevel, actual = predictNextStep(riverLevels, start + step * i)
        # actualLevels.append(actual)
        #rain.append(getRainLevelData(start + step * i - 3600, start + step * i))

        riverLevels.append(predictedRiverLevel[0])
    return riverLevels, rainLevels, timestamps, actualLevels

def loadPreviousMonthData(start):
    riverDataPrevMonth = []
    rainDataPrevMonth = []
    timestamps = []
    step = 3600
    for i in range(50):
        riverDataPrevMonth.append(getRiverLevelData(start - step * i))
        rainDataPrevMonth.append(getRainLevelDataForTime(start - step * i) / 4000)
        timestamps.append(start - step * i)
        #rain.append(getRainLevelData(start - step * i - 3600, start - step * i))

    riverLevels = list(riverDataPrevMonth[::-1])
    rainLevels = list(rainDataPrevMonth[::-1])
    actualLevels = list(riverDataPrevMonth[::-1])
    timestamps = timestamps[::-1]

    return riverLevels, rainLevels, actualLevels, timestamps

def getLatestTimestamp():
    cnx = pymysql.connect(**dbconfig)
    cursor = cnx.cursor()
    sql = "SELECT timestamp FROM rain_data ORDER BY time_string DESC LIMIT 1"
    cursor.execute(sql)
    rainLatest = cursor.fetchone()
    sql = "SELECT timestamp FROM river_data ORDER BY time_string DESC LIMIT 1"
    cursor.execute(sql)
    riverLatest = cursor.fetchone()
    cnx.close()
    if rainLatest['timestamp'] < riverLatest['timestamp']:
        return rainLatest['timestamp']

    return riverLatest['timestamp']

start = 1502561000

riverLevels, rainLevels, actualLevels, timestamps = loadPreviousMonthData(start)
riverLevels, rainLevels, timestamps, actualLevels = predictNext7days(riverLevels, rainLevels, timestamps, start)

# Create a trace
predictedTrace = go.Scatter(
    x = timestamps,
    y = riverLevels,
    name='Predicted'
)

# Create a trace
actualTrace = go.Scatter(
    x = timestamps,
    y = actualLevels,
    name='Actual'
)

rainTrace = go.Scatter(
    x = timestamps,
    y = list(map(lambda x: x, rainLevels)),
    name='Rain Level'
)


# data = [predictedTrace, actualTrace, rainTrace]
data = [predictedTrace, rainTrace, actualTrace]

# Plot and embed in ipython notebook!
py.iplot(data, filename='basic-line')
