import pymysql.cursors

config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'db': 'rainData',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

cnx = pymysql.connect(**config)
cnx.autocommit(True);