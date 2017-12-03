from datetime import datetime
import pickle
import db_config
from Prediction import Model
import numpy as np
from scipy import signal

sourceLevelMultipliers = []

def butter_lowpass(cutoff, fs, order=5):
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    return b, a

def butter_lowpass_filter(data, cutoff, fs, order=5):
    b, a = butter_lowpass(cutoff, fs, order=order)
    y = lfilter(b, a, data)
    return y

def getRiverLevelData(timestamp, riverData):
    cursor = db_config.cnx.cursor()
    sqlBefore = "SELECT * FROM rainData.river_data WHERE `timestamp` < %s AND station = %s ORDER BY `timestamp` DESC LIMIT 1"
    sqlAfter = "SELECT * FROM rainData.river_data WHERE `timestamp` > %s AND station = %s ORDER BY `timestamp` ASC LIMIT 1"

    cursor.execute(sqlBefore, (timestamp, riverData['station']))
    resultBefore = cursor.fetchone()

    cursor.execute(sqlAfter, (timestamp, riverData['station']))
    resultAfter = cursor.fetchone()

    if resultAfter == None:
        return resultBefore['river_level']

    interpolatedValue = resultBefore['river_level'] + \
        ((resultBefore['river_level'] - resultAfter['river_level']) / (resultBefore['timestamp'] - resultAfter['timestamp'])) * \
        (timestamp - resultBefore['timestamp'])

    return interpolatedValue

def getRainLevelDataForTime(timestamp, riverData):

    cursor = db_config.cnx.cursor()
    sqlBefore = "SELECT * FROM rainData.rain_data WHERE `timestamp` < %s AND area_id = %s ORDER BY `timestamp` DESC LIMIT 1"
    sqlAfter = "SELECT * FROM rainData.rain_data WHERE `timestamp` > %s AND area_id = %s ORDER BY `timestamp` ASC LIMIT 1"

    cursor.execute(sqlBefore, (timestamp, riverData['rain_radar_area_id']))
    resultBefore = cursor.fetchone()

    cursor.execute(sqlAfter, (timestamp, riverData['rain_radar_area_id']))
    resultAfter = cursor.fetchone()

    if resultAfter == None:
        return resultBefore['rain_value']

    if resultBefore == None:
        return resultAfter['rain_value']

    interpolatedValue = resultBefore['rain_value'] + \
        ((resultBefore['rain_value'] - resultAfter['rain_value']) / (resultBefore['timestamp'] - resultAfter['timestamp'])) * \
        (timestamp - resultBefore['timestamp'])

    return interpolatedValue

def matchForecastAndRealRainValues(source):
    if sourceLevelMultipliers[source]:
        return sourceLevelMultipliers[source]
    sql = 'SELECT rain_forecast_data.rain_value as forecast, rain_data.rain_value as actual FROM rain_data INNER JOIN rain_forecast_data ON `rain_forecast_data`.time_string = rain_data.time_string AND rain_data.area_id = `rain_forecast_data`.area_id WHERE `rain_forecast_data`.source = %s'
    cursor = db_config.cnx.cursor()
    cursor.execute(sql, (source))

    results = cursor.fetchall()
    values = []

    forecastAvg = sum(list(map(lambda x: x['forecast'], results))) / len(results)
    actualAvg = sum(list(map(lambda x: x['actual'], results))) / len(results)


    value = actualAvg / forecastAvg
    sourceLevelMultipliers[value]

    return value

def getRainLevelDataForTimeForecast(timestamp, riverData):
    cursor = db_config.cnx.cursor()
    sqlBefore = "SELECT * FROM rainData.rain_forecast_data WHERE `timestamp` < %s AND area_id = %s AND source IN (1, 3) ORDER BY `timestamp` DESC, `source` DESC LIMIT 1"
    sqlAfter = "SELECT * FROM rainData.rain_forecast_data WHERE `timestamp` > %s AND area_id = %s AND source IN (1, 3) ORDER BY `timestamp` ASC, `source` DESC LIMIT 1"

    cursor.execute(sqlBefore, (timestamp, riverData['rain_radar_area_id']))
    resultBefore = cursor.fetchone()

    cursor.execute(sqlAfter, (timestamp, riverData['rain_radar_area_id']))
    resultAfter = cursor.fetchone()

    if resultAfter == None:
        return resultBefore['rain_value']

    if resultBefore == None:
        return resultAfter['rain_value']

    interpolatedValue = resultBefore['rain_value'] + \
        ((resultBefore['rain_value'] - resultAfter['rain_value']) / (resultBefore['timestamp'] - resultAfter['timestamp'])) * \
        (timestamp - resultBefore['timestamp'])

    return interpolatedValue

def getRainLevelData(start, end, riverData):

    total = 0
    rainValue = 0
    while start < end:
        rainValue += getRainLevelDataForTime(start, riverData) / 10.0
        start += 3600
        total += 1

    return rainValue / total

def getRainLevelDataForecast(start, end, riverData):
    total = 0
    rainValue = 0
    while start < end:
        rainValue += getRainLevelDataForTimeForecast(start, riverData) / 10.0
        start += 3600
        total += 1

    return rainValue / total

def getFeatureForTime(timestamp, riverData, previousRiverLevels):
    PricipNow = getRainLevelDataForTimeForecast(timestamp, riverData)
    Pricip1Hour = getRainLevelDataForTimeForecast(timestamp - 3600, riverData)
    Pricip2Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 2, riverData)
    Pricip3Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 3, riverData)
    Pricip4Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 4, riverData)
    Pricip5Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 5, riverData)
    Pricip6Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 6, riverData)
    Pricip7Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 7, riverData)
    Pricip8Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 8, riverData)
    Pricip9Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 9, riverData)
    Pricip10Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 10, riverData)
    Pricip11Hour = getRainLevelDataForTimeForecast(timestamp - 3600 * 11, riverData)
    RiverLevel1HourAgo = previousRiverLevels[-1]
    RiverLevel2HourAgo = previousRiverLevels[-2]
    RiverLevel3HourAgo = previousRiverLevels[-3]
    RiverLevel4HourAgo = previousRiverLevels[-4]
    RiverLevel5HourAgo = previousRiverLevels[-5]
    RiverLevel6HourAgo = previousRiverLevels[-6]
    RiverLevel7HourAgo = previousRiverLevels[-7]
    RiverLevel8HourAgo = previousRiverLevels[-8]
    RiverLevel9HourAgo = previousRiverLevels[-9]
    RiverLevel10HourAgo = previousRiverLevels[-10]
    RiverLevel11HourAgo = previousRiverLevels[-11]
    RiverLevel12HourAgo = previousRiverLevels[-12]

    return [
    PricipNow,
    Pricip1Hour,
    Pricip2Hour,
    Pricip3Hour,
    Pricip4Hour,
    Pricip5Hour,
    Pricip6Hour,
    Pricip7Hour,
    Pricip8Hour,
    Pricip9Hour,
    Pricip10Hour,
    Pricip11Hour,
    RiverLevel1HourAgo,
    RiverLevel2HourAgo,
    RiverLevel3HourAgo,
    RiverLevel4HourAgo,
    RiverLevel5HourAgo,
    RiverLevel6HourAgo,
    RiverLevel7HourAgo,
    RiverLevel8HourAgo,
    RiverLevel9HourAgo,
    RiverLevel10HourAgo,
    RiverLevel11HourAgo,
    RiverLevel12HourAgo
    ]


def predictNextStep(previousRiverLevels, time, model, riverData):

    feature = getFeatureForTime(time, riverData, previousRiverLevels)

    predictedRiverLevel = model.predict(feature)

    return predictedRiverLevel

def predictNext7days(riverLevels, timestamps, start, riverData, model):
    step = 3600
    actualLevels = []

    for i in range(168):
        timestamps.append(start + step * (i + 1))
        # rainLevels.append(getRainLevelDataForTimeForecast(start + step * i))
        # rainLevels.append(getRainLevelDataForTimeForecast(start + step * i))
        # rainLevels.append(getRainLevelDataForTimeForecast(start + step * i))
        # rainLevels.append(getRainLevelDataForTimeForecast(start + step * i))
        predictedRiverLevel = predictNextStep(riverLevels, start + step * i, model, riverData)
        #actualLevels.append(actual)
        #rain.append(getRainLevelData(start + step * i - 3600, start + step * i))
        # print(predictedRiverLevel)
        riverLevels.append(predictedRiverLevel)
    return riverLevels, timestamps

def loadPreviousData(start, riverData):
    riverDataPrevMonth = []
    timestamps = []
    step = 3600
    for i in range(50):
        riverDataPrevMonth.append(getRiverLevelData(start - step * i, riverData))
        # rainDataPrevMonth.append(getRainLevelDataForTime(start - step * i))
        # rainDataPrevMonth.append(getRainLevelDataForTime(start - step * i))
        # rainDataPrevMonth.append(getRainLevelDataForTime(start - step * i))
        # rainDataPrevMonth.append(getRainLevelDataForTime(start - step * i))
        timestamps.append(start - step * i)
        #rain.append(getRainLevelData(start - step * i - 3600, start - step * i))

    riverLevels = list(riverDataPrevMonth[::-1])
    # rainLevels = list(rainDataPrevMonth[::-1])
    actualLevels = list(riverDataPrevMonth[::-1])
    timestamps = timestamps[::-1]

    return riverLevels, actualLevels, timestamps

def getLatestTimestamp(riverData):
    cursor = db_config.cnx.cursor()
    sql = "SELECT timestamp FROM rain_data ORDER BY time_string DESC LIMIT 1"
    cursor.execute(sql)
    rainLatest = cursor.fetchone()
    sql = "SELECT timestamp FROM river_data WHERE station = %s ORDER BY time_string DESC LIMIT 1"
    cursor.execute(sql, (riverData['station']))
    riverLatest = cursor.fetchone()

    if rainLatest['timestamp'] < riverLatest['timestamp']:
        return rainLatest['timestamp']

    return riverLatest['timestamp']

def writePredictionToDb(riverLevels, timestamps, start, riverData, live):
    predictRunTime = datetime.now()
    cursor = db_config.cnx.cursor()
    sql = "INSERT INTO predictions (created_date, model_id, river_id, live) VALUES (%s, %s, %s, %s)"
    print(riverData)
    cursor.execute(sql, (predictRunTime, riverData['model_id'], riverData['id'], live))
    prediction_id = cursor.lastrowid

    timestamps = list(filter(lambda x: x > start, timestamps))
    riverLevels = riverLevels[-len(timestamps):]
    riverLevels = filterRiverLevel(riverLevels)
    for i in range(len(riverLevels)):
        sql = "REPLACE INTO predicted_river_levels (predict_time, prediction_id, river_level) VALUES (%s, %s, %s)"
        cursor.execute(sql, (datetime.fromtimestamp(timestamps[i]), prediction_id, float(riverLevels[i])))
        db_config.cnx.commit()

def getRiverData(riverId, configId):
    print(riverId, configId)
    cursor = db_config.cnx.cursor()
    sql = 'SELECT *, models.id as `model_id` FROM rivers INNER JOIN models ON models.river_id = rivers.id AND model_config_id = %s WHERE rivers.id = %s ORDER BY models.id DESC LIMIT 1'
    cursor.execute(sql, (configId, riverId))
    return cursor.fetchone()

def predict(riverId, configId=1, live=1, model=None, start=None):
    deleteOldPrediction(riverId)
    riverData = getRiverData(riverId, configId)
    print(riverData)
    if start == None:
        start = getLatestTimestamp(riverData)
    print(start)
    riverLevels = []
    timestamps = []
    riverLevels, actualLevels, timestamps = loadPreviousData(start, riverData)

    if model == None:
        model = Model.getModel(riverId, configId)

    riverLevels, timestamps = predictNext7days(riverLevels, timestamps, start, riverData, model)
    writePredictionToDb(riverLevels, timestamps, start, riverData, live)

def deleteOldPrediction(riverId):
    cursor = db_config.cnx.cursor()
    sql = 'DELETE FROM predicted_river_levels WHERE predict_time < DATE_ADD(NOW(), INTERVAL -1 MONTH)'
    cursor.execute(sql)
    db_config.cnx.commit()

def filterRiverLevel(riverLevels, alpha=1):
    filteredRiverLevels = []
    for i in range(alpha, len(riverLevels) - alpha - 1):
        value = sum(riverLevels[i-alpha:i+alpha+1]) / (alpha * 2 + 1)
        filteredRiverLevels.append(value)
    return filteredRiverLevels
