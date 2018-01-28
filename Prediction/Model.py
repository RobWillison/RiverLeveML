import db_config
import matplotlib.pyplot as plt
import numpy as np
from Prediction.RobModel import RobModel

def loadRiverData(riverData):
    sql = "SELECT river_level, timestamp FROM river_data WHERE river_id = %s ORDER BY timestamp DESC LIMIT 500"
    cursor = db_config.cnx.cursor()
    cursor.execute(sql, (riverData['river_id']))
    return cursor.fetchall()

def excludeRunOff(datas):
    results = []
    for i in range(len(datas) - 1):
        if datas[i]['river_level'] <= datas[i+1]['river_level']:
            continue
        results.append(datas[i])
    return results

def getSpeed(datas):
    results = []
    for i in range(len(datas) - 1):
        datas[i]['speed'] = datas[i]['river_level'] - datas[i+1]['river_level']
        results.append(datas[i])
    return results

def totalRainBetween(start, end, riverData):
    cursor = db_config.cnx.cursor()
    sqlBefore = "SELECT SUM(rain_value) / COUNT(rain_value) as total FROM rainData.rain_data WHERE `timestamp` BETWEEN %s AND %s AND area_id = %s"

    cursor.execute(sqlBefore, (start, end, riverData['rain_radar_area_id']))
    resultBefore = cursor.fetchone()
    if resultBefore['total'] == None:
        raise 'None Error'
    return resultBefore['total']

def getRunOnFit(riverData):
    history = 3600 * 6
    datas = loadRiverData(riverData)
    datas = excludeRunOff(datas)

    y = []
    x = []
    for data in datas:
        level = data['river_level']
        total = totalRainBetween(data['timestamp'] - history, data['timestamp'], riverData)
        y.append(float(level))
        x.append(float(total))

    y = np.array(y)
    x = np.array(x)

    fit = np.polyfit(x, y, deg=1)
    return np.poly1d(fit)

def getRunOffFit(riverData):
    history = 3600 * 6
    datas = loadRiverData(riverData)
    datas = getSpeed(datas)

    y = []
    x = []

    for data in datas:
        level = data['river_level']
        total = totalRainBetween(data['timestamp'] - history, data['timestamp'], riverData)
        if total != 0:
            continue
        x.append(float(level))
        y.append(float(data['speed']))


    x = np.array(x)
    y = np.array(y)

    fit = np.polyfit(x, y, deg=1)
    return np.poly1d(fit)

def train(riverData):
    runOnFit = getRunOnFit(riverData)
    runOffFit = getRunOffFit(riverData)

    return RobModel(runOnFit, runOffFit)
