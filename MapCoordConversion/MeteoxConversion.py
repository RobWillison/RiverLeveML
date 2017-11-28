from convertbng.util import convert_bng, convert_lonlat
import db_config
import math

def getMapPixelData():
    return [[320, 350], 680 / 70, 1100 / 130]

def eastNorthToPixel(easting, northing):
    startCoord, xStep, yStep = getMapPixelData()

    pixelX = (xStep * easting) + startCoord[0]
    pixelY = (yStep * northing) + startCoord[1]

    return [pixelX, pixelY]

def pixelToEastNorth(pixelX, pixelY):
    startCoord, xStep, yStep = getMapPixelData()
    easting = (pixelX - startCoord[0]) / xStep
    northing = (pixelY - startCoord[1]) / yStep

    return easting, northing


def getEastingNorthing(lat, long):
    return convert_bng(long, lat)

def getRainArea(mapCoords):
    xGrid = math.floor(mapCoords[0] / 50)
    yGrid = math.floor((1450 - mapCoords[1]) / 50)

    return (yGrid * 40) + xGrid

def convertToLatLong(areaId):
    yCord = (math.floor(areaId / 40)) * 50
    xCord = (areaId - ((yCord / 50) * 40)) * 50
    easting, northing = pixelToEastNorth(xCord, yCord)
    print(easting, northing)
    result = convert_lonlat(easting, northing)
    print(result)

def getRiverInfo():
    cursor = db_config.cnx.cursor()

    sql = "SELECT * FROM rivers WHERE rain_radar_area_id IS NULL AND lat IS NOT NULL"
    cursor.execute(sql)
    result = cursor.fetchall()

    return result

def updateRainArea(id, rainArea):
    cursor = db_config.cnx.cursor()

    sql = "UPDATE rivers SET rain_radar_area_id = %s WHERE id = %s"
    cursor.execute(sql, (rainArea, id))
    db_config.cnx.commit()

def updateRivers():
    for river in getRiverInfo():
        gridref = getEastingNorthing(river['lat'], river['long'])
        easting = gridref[0][0] / 10000
        northing = gridref[1][0] / 10000

        mapCoords = eastNorthToPixel(easting, northing)
        areaId = getRainArea(mapCoords)
        print(areaId)
        updateRainArea(river['id'], areaId)

convertToLatLong(811)