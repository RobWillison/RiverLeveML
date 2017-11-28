import datetime
import png
import time
import pymysql.cursors
import base64
import io
from operator import add
import math
from PIL import Image
import os

sofar = []

def processForecastImage(fileName, sourceInfo):
    fileType = fileName[-3:]
    f = open(fileName, 'rb')

    im = Image.open(fileName)
    if fileType == 'gif':
        im = im.convert('RGB')

    imageData = list(im.getdata())

    width, height = im.size

    imageData = [imageData[i * width:(i + 1) * width] for i in range(height)]

    result = getRainForecastValueBlocks([width, height, imageData], sourceInfo)
    os.remove(fileName)
    return result

def getRainForecastValueBlocks(imageData, sourceInfo):
    imageArray = imageData[2]
    numYBlocks = int(imageData[0] / sourceInfo['blockSize'])
    numXBlocks = int(imageData[1] / sourceInfo['blockSize'])

    rainBlocks = [[0] * numYBlocks for i in range(numXBlocks)]

    for y in range(imageData[1]):
        for x in range(imageData[0]):
            if sourceInfo['type'] == 'Meteo':
                rainForPixel = RGBAtommMeteo(imageArray[y][x])
            elif sourceInfo['type'] == 'WeatherOnline':
                rainForPixel = RGBAtommWeatherOnline(imageArray[y][x])

            rainBlocks[math.floor(y / sourceInfo['blockSize'])][math.floor(x / sourceInfo['blockSize'])] += rainForPixel

    return rainBlocks

def RGBAtommMeteo(data):
    r, g, b = data

    if((b == 255) and (r >= 213) and (g >= 231)):
        return 1
    if((b == 251) and (r <= 143) and (g <= 194)):
        return 2
    if((b == 252) and (r <= 106) and (g <= 147)):
        return 3
    if((b == 237) and (r <= 88) and (g <= 126)):
        return 4
    if((b == 252) and (r <= 1) and (g <= 126)):
        return 5
    if((b == 139) and (r <= 3) and (g <= 151)):
        return 6
    if((b == 67) and (r <= 62) and (g <= 90)):
        return 7

    return 0

def RGBAtommWeatherOnline(data):
    r, g, b = data
    if r == 170 and g == 255 and b == 255:
        return 1
    if r == 85 and g == 213 and b == 255:
        return 2
    if r == 42 and g == 170 and b == 255:
        return 5
    if r == 42 and g == 127 and b == 255:
        return 10
    if r == 0 and g == 85 and b == 192:
        return 15
    if r == 0 and g == 42 and b == 192:
        return 20
    if r == 170 and g == 0 and b == 127:
        return 30

    return 0
