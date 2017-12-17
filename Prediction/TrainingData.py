import numpy as np
import pickle
import db_config
from sklearn.preprocessing import MinMaxScaler

def getRiverLevelData(timestamp, riverData, scaler):
    cursor = db_config.cnx.cursor()
    sqlBefore = "SELECT * FROM rainData.river_data WHERE `timestamp` < %s AND river_id = %s ORDER BY `timestamp` DESC LIMIT 1"
    sqlAfter = "SELECT * FROM rainData.river_data WHERE `timestamp` > %s AND river_id = %s ORDER BY `timestamp` ASC LIMIT 1"

    cursor.execute(sqlBefore, (timestamp, riverData['id']))
    resultBefore = cursor.fetchone()

    cursor.execute(sqlAfter, (timestamp, riverData['id']))
    resultAfter = cursor.fetchone()

    if resultAfter == None:
        return resultBefore['river_level']

    interpolatedValue = resultBefore['river_level'] + \
        ((resultBefore['river_level'] - resultAfter['river_level']) / (resultBefore['timestamp'] - resultAfter['timestamp'])) * \
        (timestamp - resultBefore['timestamp'])

    return scaler.transform([[interpolatedValue]])[0][0]

def getRainLevelDataForTime(timestamp, riverData, scaler):
    cursor = db_config.cnx.cursor()
    sqlBefore = "SELECT * FROM rainData.rain_data WHERE `timestamp` < %s AND area_id = %s ORDER BY `timestamp` DESC LIMIT 1"
    sqlAfter = "SELECT * FROM rainData.rain_data WHERE `timestamp` > %s AND area_id = %s ORDER BY `timestamp` ASC LIMIT 1"

    cursor.execute(sqlBefore, (timestamp, riverData['rain_radar_area_id']))
    resultBefore = cursor.fetchone()

    cursor.execute(sqlAfter, (timestamp, riverData['rain_radar_area_id']))
    resultAfter = cursor.fetchone()

    if resultAfter == None:
        return resultBefore['rain_value']

    interpolatedValue = resultBefore['rain_value'] + \
        ((resultBefore['rain_value'] - resultAfter['rain_value']) / (resultBefore['timestamp'] - resultAfter['timestamp'])) * \
        (timestamp - resultBefore['timestamp'])

    return scaler.transform([[interpolatedValue]])[0][0]

def fitRainMinMaxScaler():
    cursor = db_config.cnx.cursor()
    sql = 'SELECT rain_value FROM rain_data LIMIT 5000'
    cursor.execute(sql)
    results = list(map(lambda x: [x['rain_value']], cursor.fetchall()))
    scaler = MinMaxScaler()
    scaler.fit(results)

    return scaler

def fitRiverLevelMinMaxScaler(station):
    cursor = db_config.cnx.cursor()
    sql = 'SELECT river_level FROM river_data WHERE station = %s LIMIT 5000'
    cursor.execute(sql, (station))
    results = list(map(lambda x: [x['river_level']], cursor.fetchall()))
    scaler = MinMaxScaler()
    scaler.fit(results)

    return scaler

# P – Precipitation today
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

def getFeatureForTime(timestamp, riverData, riverScaler, rainScaler):
    PricipNow = getRainLevelDataForTime(timestamp, riverData, rainScaler)
    Pricip1Hour = getRainLevelDataForTime(timestamp - 3600, riverData, rainScaler)
    Pricip2Hour = getRainLevelDataForTime(timestamp - 3600 * 2, riverData, rainScaler)
    Pricip3Hour = getRainLevelDataForTime(timestamp - 3600 * 3, riverData, rainScaler)
    Pricip4Hour = getRainLevelDataForTime(timestamp - 3600 * 4, riverData, rainScaler)
    Pricip5Hour = getRainLevelDataForTime(timestamp - 3600 * 5, riverData, rainScaler)
    Pricip6Hour = getRainLevelDataForTime(timestamp - 3600 * 6, riverData, rainScaler)
    RiverLevel1HourAgo = getRiverLevelData(timestamp - 3600 * 1, riverData, riverScaler)
    RiverLevel2HourAgo = getRiverLevelData(timestamp - 3600 * 2, riverData, riverScaler)
    RiverLevel3HourAgo = getRiverLevelData(timestamp - 3600 * 3, riverData, riverScaler)
    RiverLevel4HourAgo = getRiverLevelData(timestamp - 3600 * 4, riverData, riverScaler)
    RiverLevel5HourAgo = getRiverLevelData(timestamp - 3600 * 5, riverData, riverScaler)
    RiverLevel6HourAgo = getRiverLevelData(timestamp - 3600 * 6, riverData, riverScaler)
    print(timestamp)
    actual = getRiverLevelData(timestamp, riverData, riverScaler)

    return [
    PricipNow,
    Pricip1Hour,
    Pricip2Hour,
    Pricip3Hour,
    Pricip4Hour,
    Pricip5Hour,
    RiverLevel1HourAgo,
    RiverLevel2HourAgo,
    RiverLevel3HourAgo,
    RiverLevel4HourAgo,
    RiverLevel5HourAgo,
    RiverLevel6HourAgo,
    ], actual

def getLatestTimestamp(areaId, riverId):
    cursor = db_config.cnx.cursor()
    sql = "SELECT timestamp FROM rain_data WHERE area_id = %s ORDER BY time_string DESC LIMIT 1"
    cursor.execute(sql, (areaId))
    rainLatest = cursor.fetchone()
    sql = "SELECT timestamp FROM river_data WHERE river_id = %s ORDER BY time_string DESC LIMIT 1"
    cursor.execute(sql, (riverId))
    riverLatest = cursor.fetchone()

    if rainLatest['timestamp'] < riverLatest['timestamp']:
        return rainLatest['timestamp']

    return riverLatest['timestamp']

def getEarliestTimestamp(areaId, riverId):
    cursor = db_config.cnx.cursor()
    sql = "SELECT timestamp FROM rain_data WHERE area_id = %s ORDER BY timestamp ASC LIMIT 1"
    cursor.execute(sql, (areaId))
    rainLatest = cursor.fetchone()
    sql = "SELECT timestamp FROM river_data WHERE river_id = %s ORDER BY timestamp ASC LIMIT 1"
    cursor.execute(sql, (riverId))
    riverLatest = cursor.fetchone()

    if rainLatest['timestamp'] > riverLatest['timestamp']:
        return rainLatest['timestamp']

    return riverLatest['timestamp']

def getRiverData(riverId):
    cursor = db_config.cnx.cursor()
    sql = 'SELECT * FROM rivers WHERE id = %s'
    cursor.execute(sql, (riverId))
    return cursor.fetchone()

def updateTrainingDataFile(riverData):
    start = getLatestTimestamp(riverData['rain_radar_area_id'], riverData['id'])
    end = getEarliestTimestamp(riverData['rain_radar_area_id'], riverData['id'])
    trainingData, outputValues, rainScaler, riverScaler = getTrainingData(start, end, riverData)

    data = pickle.dumps([trainingData, outputValues, rainScaler, riverScaler])
    print(len(trainingData), len(trainingData[0]), len(trainingData[0][0]))
    saveData(data, riverData['id'])

def getTrainingData(start, end, riverData):
    end = end + (12 * 3600)
    trainingData = []
    outputValues = []
    rainScaler = fitRainMinMaxScaler()
    riverScaler = fitRiverLevelMinMaxScaler(riverData['station'])

    #Max 8447
    while start > end:
        feature, actual = getFeatureForTime(start, riverData, riverScaler, rainScaler)
        start -= 3600
        trainingData.append([feature])
        outputValues.append(actual)
    print('Traing Data Length' + str(len(trainingData)))
    return trainingData, outputValues, rainScaler, riverScaler

def saveData(data, river_id):
    cursor = db_config.cnx.cursor()
    sql = 'REPLACE INTO training_data (data, river_id) VALUES (%s, %s)'
    cursor.execute(sql, (data, river_id))
    db_config.cnx.commit()

def updateRiver(riverId):
    river = getRiverData(riverId)

    print(river)

    updateTrainingDataFile(river)
