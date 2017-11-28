import plotly.plotly as py
import plotly.graph_objs as go
import datetime
import time
import pymysql.cursors
import numpy
import numpy as np
from scipy.signal import butter, lfilter, freqz
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
import pickle

dbconfig = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'db': 'rainData',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def getRiverLevelDataForTimestamp(timestamp):
    timestamps = []
    riverValues = []

    cnx = pymysql.connect(**dbconfig)

    cursor = cnx.cursor()

    sql = "SELECT * FROM `riverData` order by ABS(timestamp - %s) ASC limit 1"
    cursor.execute(sql, (timestamp))

    result = cursor.fetchone()

    if (result != None):
        cnx.commit()
        cnx.close()
        return result['riverLevel']



    return None

def getRainLevelDataForTimestamp(timestampStart, timestampEnd):
    timestamps = []
    rainValues = []

    cnx = pymysql.connect(**dbconfig)

    cursor = cnx.cursor()

    sql = "SELECT * FROM `rainData` WHERE `timestamp` BETWEEN %s AND %s"
    cursor.execute(sql, (timestampStart, timestampEnd))

    result = cursor.fetchone()

    while (result != None):

        #if (result['rainValue'] == 500):
        #    continue

        rainValues.append(result['rainValue'] / 10)

        result = cursor.fetchone()

    cnx.commit()

    cnx.close()

    return rainValues

clf = pickle.load(open("classifier.clf", "rb"))

#testing
start = 1481000000
end = 1484500000
step = 7200

actualTimestamps = []
actualRiver = []
predictedDirection = []


while (start < end):
    actualTimestamps.append(start)
    actualRiver.append(getRiverLevelDataForTimestamp(start))

    stepValues = [0, 7200, 14400, 21600, 28800, 36000, 43200, 50400, 57600]
    values = []
    difference = 0
    for timeStep in stepValues:
        values.append(getRiverLevelDataForTimestamp(start + timeStep))

    rainAmount = getRainLevelDataForTimestamp(start, start + step)

    if (len(values) == 0 or len(rainAmount) == 0):
        start += step
        predictedDirection.append(0)
        continue

    values.append(sum(rainAmount) / len(rainAmount))
    inputData = values

    start += step

    actual = clf.predict([inputData])
    predictedDirection.append(int(actual[0]))

# Create a trace
actualrivertrace = go.Scatter(
    x = actualTimestamps,
    y = actualRiver,
    name='Actual River Level'
)

# Create a trace
predictedrivertrace = go.Scatter(
    x = actualTimestamps,
    y = predictedDirection,
    name='Predicted River Level'
)

data = [actualrivertrace, predictedrivertrace]

# Plot and embed in ipython notebook!
py.iplot(data, filename='basic-line')

