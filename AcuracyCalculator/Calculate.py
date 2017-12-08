import db_config
from datetime import datetime, timedelta
import json

def calculateAcuracy(riverData, predictionId, timeStart, timeEnd):
    sql = 'SELECT river_level FROM predicted_river_levels WHERE prediction_id = %s AND predict_time BETWEEN %s AND %s'
    cursor = db_config.cnx.cursor()

    cursor.execute(sql, (predictionId, timeStart.strftime("%Y-%m-%d %H:%M:%S"), timeEnd.strftime("%Y-%m-%d %H:%M:%S")))
    prediction = list(map(lambda x: x['river_level'], cursor.fetchall()))
    print(riverData['station'])
    sql = 'SELECT river_level FROM river_data WHERE station = %s AND time_string BETWEEN %s AND %s'

    cursor.execute(sql, (riverData['station'], timeStart.strftime("%Y-%m-%d %H:%M:%S"), timeEnd.strftime("%Y-%m-%d %H:%M:%S")))
    actual = list(map(lambda x: x['river_level'], cursor.fetchall()))
    print(actual)
    if len(actual) == 0:
        return False

    actualAvg = sum(actual) / len(actual)
    predictionAvg = sum(prediction) / len(prediction)

    error = abs(predictionAvg - actualAvg)
    return (error / actualAvg) * 100


def getRiverData(riverId):
    cursor = db_config.cnx.cursor()
    sql = 'SELECT * FROM rivers WHERE id = %s'
    cursor.execute(sql, (riverId))
    return cursor.fetchone()

def getPredictions():
    cursor = db_config.cnx.cursor()
    sql = 'SELECT * FROM predictions WHERE predictions.created_date < CURDATE() - INTERVAL 8 DAY AND acuracy_info IS NULL'
    cursor.execute(sql)
    result = cursor.fetchall()
    if result == None:
        return None

    return result

def writeAcuracyToDb(data, predictionId):
    jsonData = json.dumps(data)
    cursor = db_config.cnx.cursor()
    sql = 'UPDATE predictions SET acuracy_info = %s WHERE id = %s'
    cursor.execute(sql, (jsonData, predictionId))
    db_config.cnx.commit()

def calculateAcuracys(riverData, predictionId):
    print(predictionId)
    cursor = db_config.cnx.cursor()
    cursor.execute('SELECT predict_time FROM predicted_river_levels WHERE prediction_id = %s ORDER BY predict_time ASC LIMIT 1', (predictionId))
    startTime = cursor.fetchone()['predict_time']
    cursor.execute('SELECT predict_time FROM predicted_river_levels WHERE prediction_id = %s ORDER BY predict_time DESC LIMIT 1', (predictionId))
    endTime = cursor.fetchone()['predict_time']
    print(startTime)
    print(endTime)
    hour = 1
    data = {}
    while startTime < endTime:
        nextTime = startTime + timedelta(hours=1)
        error = calculateAcuracy(riverData, predictionId, startTime, nextTime)

        if not error:
            return None

        data[hour] = error
        hour += 1
        startTime = nextTime

    return data


def calculateAllAcuracys():
    predictions = getPredictions()

    for prediction in predictions:
        if prediction['river_id'] == None:
            continue

        river = getRiverData(prediction['river_id'])
        try:
            data = calculateAcuracys(river, prediction['id'])
            if data != None:
                writeAcuracyToDb(data, prediction['id'])
        except Exception as ex:
            print(ex)
