import db_config
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit

def loadRiverData():
    sql = "SELECT river_level, timestamp FROM river_data WHERE river_id = 1 ORDER BY timestamp DESC LIMIT 500"
    cursor = db_config.cnx.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def excludeRunOff(datas):
    results = []
    for i in range(len(datas) - 1):
        if datas[i]['river_level'] <= datas[i+1]['river_level']:
            continue
        print(datas[i])
        results.append(datas[i])
    return results

def getSpeed(datas):
    results = []
    for i in range(len(datas) - 1):
        datas[i]['speed'] = datas[i]['river_level'] - datas[i+1]['river_level']
        results.append(datas[i])
    return results

def totalRainBetween(start, end):
    cursor = db_config.cnx.cursor()
    sqlBefore = "SELECT SUM(rain_value) / COUNT(rain_value) as total FROM rainData.rain_data WHERE `timestamp` BETWEEN %s AND %s AND area_id = %s"

    cursor.execute(sqlBefore, (start, end, 811))
    resultBefore = cursor.fetchone()
    if resultBefore['total'] == None:
        raise 'None Error'
    return resultBefore['total']

def analyseUp():
    history = 3600 * 6
    datas = loadRiverData()
    datas = excludeRunOff(datas)

    y = []
    x = []
    for data in datas:
        level = int(data['river_level'] * 100)
        total = totalRainBetween(data['timestamp'] - history, data['timestamp'])
        x.append(float(level))
        y.append(float(total))

    x = np.array(x)
    y = np.array(y)

    fig, ax = plt.subplots()
    popt, pcov = curve_fit(func, x, y)
    ax.plot(x, func(x, *popt), color='red')
    ax.scatter(x, y)

    fig.show()

def analyseDown():
    history = 3600 * 6
    datas = loadRiverData()
    print('1')
    print(len(datas))
    datas = getSpeed(datas)
    print(len(datas))
    y = []
    x = []
    print(datas)
    for data in datas:
        level = data['river_level']
        total = totalRainBetween(data['timestamp'] - history, data['timestamp'])
        if total != 0:
            continue
        x.append(float(level))
        y.append(float(data['speed']))
    print('1')

    x = np.array(x)
    y = np.array(y)

    fig, ax = plt.subplots()
    popt, pcov = curve_fit(func, x, y)
    ax.plot(x, func(x, *popt), color='red')
    ax.scatter(x, y)

    fig.show()

def func(x, a, b, c):
    return a*x**2 + b*x + c
