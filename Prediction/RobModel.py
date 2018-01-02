import pickle
import db_config
from keras.models import load_model
import numpy as np

class RobModel:
    model = None
    riverId = None
    configId = None

    def __init__(self, riverId, configId=1):
        self.riverId = riverId
        self.configId = configId

    def set_model(self, model):
        self.model = model
        return self

    def load(self):
        cursor = db_config.cnx.cursor()
        sql = 'SELECT model_path FROM models WHERE model_config_id = %s AND river_id = %s ORDER BY id desc LIMIT 1'
        cursor.execute(sql, (self.configId, self.riverId))
        result = cursor.fetchone()
        self.model = load_model(result['model_path'])
        return self


    def save(self):
        path = 'Data/Models/river' + str(self.riverId) + '-' + str(self.configId) + '.h5'
        self.model.save(path)
        self.add_model(path)

        cursor = db_config.cnx.cursor()

        return self

    def add_model(self, path):
        cursor = db_config.cnx.cursor()
        sql = 'REPLACE INTO models (river_id, model_config_id, model_path, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())'
        cursor.execute(sql, (self.riverId, self.configId, path))
        db_config.cnx.commit()

    def predict(self, feature):
        print(feature)
        prediction = self.model.predict(np.array([feature]))[0][0]
        print(prediction)
        return prediction
