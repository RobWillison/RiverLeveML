import db_config
from datetime import datetime, timedelta
from DataGatherer import RainGatherer
from DataGatherer import RiverLevelGatherer
from DataGatherer import RainForecastGatherer
from Prediction import TrainingData
from Prediction import Model
from Prediction import Predict
import time
import traceback
import subprocess

def setLastRun(job):
    cursor = db_config.cnx.cursor()
    sql = "UPDATE jobs SET last_run = %s WHERE id = %s"
    cursor.execute(sql, (datetime.now(), job['id']))
    db_config.cnx.commit()

def setStartRun(job):
    cursor = db_config.cnx.cursor()
    sql = "UPDATE jobs SET last_start_time = %s WHERE id = %s"
    cursor.execute(sql, (datetime.now(), job['id']))
    print(job['id'])
    db_config.cnx.commit()

def setError(job, error):
    cursor = db_config.cnx.cursor()
    sql = "UPDATE jobs SET error_message = %s WHERE id = %s"
    cursor.execute(sql, (str(error), job['id']))
    db_config.cnx.commit()

def nextJob():
    sql = 'SELECT * FROM jobs WHERE last_run < DATE_ADD(NOW(), INTERVAL -`run_frequency` HOUR) OR last_run IS NULL ORDER BY priority, last_run ASC LIMIT 1'
    cursor = db_config.cnx.cursor()
    cursor.execute(sql, ())
    return cursor.fetchone()

def RunCommand(command):
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(output)

def run():
    while (True):
        job = nextJob()
        if job == None:
            print('No Jobs')
            time.sleep(60)
            continue
        print(job)
        setStartRun(job)
        try:
            eval(job['call'])
        except Exception as e:
            setError(job, traceback.format_exc())
        finally:
            setError(job, '')
            setLastRun(job)