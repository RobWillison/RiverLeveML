import db_config
from datetime import datetime, timedelta
from DataGatherer import RainGatherer
from DataGatherer import RiverLevelGatherer
from DataGatherer import RainForecastGatherer
from Prediction import TrainingData
from Prediction import Model
from Prediction import Predict
from AcuracyCalculator import Calculate
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

def nextJob(previous):
    cursor = db_config.cnx.cursor()
    if previous != None:
        sql = 'SELECT * FROM jobs WHERE id IN (SELECT run_after FROM jobs WHERE id = %s)'
        cursor.execute(sql, (previous['id']))
        result = cursor.fetchone()
        if result != None:
            return result

    sql = 'SELECT * FROM jobs WHERE last_run < DATE_ADD(NOW(), INTERVAL -`run_frequency` HOUR) OR last_run IS NULL ORDER BY (priority - TIMESTAMPDIFF(HOUR, last_start_time, NOW())), last_run ASC LIMIT 1'

    cursor.execute(sql, ())
    return cursor.fetchone()

def RunCommand(command):
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(output)

def finish(job):
    setError(job, '')
    setLastRun(job)

def run():
    job = None
    while (True):
        job = nextJob(job)
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
            finish(job)