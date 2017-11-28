import io
import urllib.request
import datetime
import png
import time
from multiprocessing import Process, Lock
import base64
import io
from operator import add
import glob
import math
import os
import db_config

def getRainDataImg(fileName):

    timeString = fileName[10:-4]
    timestamp = datetime.datetime.strptime(timeString, '%Y%m%d%H%M')

    if checkIfDateHasBeenDone(timestamp):
        return

    f = open(fileName, 'rb')
    imageReader = png.Reader(file = f)
    imageData = imageReader.read()

    values = getRainValueBlocks(imageData, 50)


    saveData(timestamp, values)

def checkIfDateHasBeenDone(timestamp):
    sql = "SELECT id FROM rain_data WHERE time_string = %s"

    cursor = db_config.cnx.cursor()

    cursor.execute(sql, (timestamp.strftime("%Y-%m-%d %H:%M")))
    result = cursor.fetchone()

    return result != None


def getRainValueBlocks(imageData, blockSize):
    imageArray = list(imageData[2])
    rainBlocks = [[0] * 40 for i in range(29)]


    for y in range(imageData[1]):
        for x in range(imageData[0]):
            if(len(imageArray[0]) == 2000):
                palete = imageData[3]['palette']
                #using Indexed colour, with a palete
                indexColour = imageArray[y][x]

                rainForPixel =  RGBAtomm(palete[indexColour])
                rainBlocks[math.floor(y / 50)][math.floor(x / 50)] += rainForPixel
                continue


            rainForPixel = RGBAtomm([imageArray[y][x * 4], imageArray[y][(x * 4) + 1], imageArray[y][(x * 4) + 2], imageArray[y][(x * 4) + 3]])

            rainBlocks[math.floor(y / 50)][math.floor(x / 50)] += rainForPixel

    return rainBlocks

def RGBAtomm(data):
    r, g, b, a = data
    if(a == 0):
        return 0

    if((b == 255) and (r >= 158) and (g >= 158)):
        #0 - 2mm
        #green and red go from 255 to 158 from 0 mm to 2mm
        step = 2 / (255 - 158)
        return 2 - ((r - 158) * step)

    if((b == 255) and (r <= 110) and (g <= 110)):
        #2 - 5mm
        #green and red go from 110 to 88 from 2 mm to 5mm
        step = 3 / (110 - 88)
        return 5 - ((r - 88) * step)


    if((b <= 200) and (b >= 110)):
        #5 - 10mm
        # b goes from 200 to 110 as rain 5mm to 10mm
        step = 5 / (200 - 110)
        return 10 - ((b - 110) * step)

    if((r <= 255) and (r >= 131)):
        #10 - 25mm
        step = 15 / (255 - 131)
        return 25 - (r - 131) * step

    if((r == 192) and (b == 192)):
        #25mm +
        #?? What do I do here :P
        return 25

    return 500

def saveData(dateTime, rainValues):
    timeString = dateTime.strftime("%Y-%m-%d %H:%M")
    timestamp = time.mktime(dateTime.timetuple())

    cursor = db_config.cnx.cursor()
    areaId = 0
    for y in range(len(rainValues)):
        for x in range(len(rainValues[y])):
            sql = "INSERT IGNORE INTO `rain_data` (`timestamp`, `time_string`, `rain_value`, `area_id`) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (timestamp, timeString, rainValues[y][x], areaId))

            areaId += 1
            db_config.cnx.commit()


def runPopulation(listOfFiles):

    for fileName in listOfFiles:
        getRainDataImg(fileName)
        os.remove(fileName)

def processImages(imageFileNames):
    runPopulation(imageFileNames)
