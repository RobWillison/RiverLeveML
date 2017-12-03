from Prediction import TrainingData
from Prediction import Model
from Prediction import Predict
from AcuracyCalculator import Calculate
import db_config

def selectRandomDates(number):
    cursor = db_config.cnx.cursor()
    sql = 'SELECT predict_time FROM predicted_river_levels ORDER BY RAND() LIMIT %s'
    cursor.execute(sql, (number))
    return cursor.fetchall()

def testModelConfig(configId, num=100):
    dates = selectRandomDates(num)
    riverId = 1
    for date in dates:
        date = date['predict_time'].timestamp()
        riverData = TrainingData.getRiverData(riverId)
        data = TrainingData.getTrainingData(date, riverData)
        model = Model.train(riverId, configId, data)
        Predict.predict(riverId, configId, 0, model, date)
        Calculate.calculateAllAcuracys()