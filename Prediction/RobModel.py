import pickle
import db_config
from keras.models import load_model
import numpy as np

class RobModel:
    model = None
    rainScaler = None
    riverScaler = None
    riverId = None
    configId = None

    def __init__(self, riverId, configId=1):
        self.riverId = riverId
        self.configId = configId

    def set_model(self, model, rainScaler, riverScaler):
        self.model = model
        self.rainScaler = rainScaler
        self.riverScaler = riverScaler
        return self

    def load(self):
        cursor = db_config.cnx.cursor()
        sql = 'SELECT model_path, scalers FROM models WHERE model_config_id = %s AND river_id = %s'
        cursor.execute(sql, (self.configId, self.riverId))
        result = cursor.fetchone()
        scalers = pickle.loads(result['scalers'])
        self.model = load_model(result['model_path'])
        self.rainScaler = scalers[0]
        self.riverScaler = scalers[1]
        return self


    def save(self):
        path = 'Data/Models/river' + str(self.riverId) + '-' + str(self.configId) + '.h5'
        self.model.save(path)
        scalers = [self.rainScaler, self.riverScaler]
        scalers = pickle.dumps(scalers)

        self.add_model(path, scalers)

        cursor = db_config.cnx.cursor()

        return self

    def add_model(self, path, scalers):
        cursor = db_config.cnx.cursor()
        sql = 'REPLACE INTO models (river_id, model_config_id, model_path, scalers, created_at, updated_at) VALUES (%s, %s, %s, %s, NOW(), NOW())'
        cursor.execute(sql, (self.riverId, self.configId, path, scalers))
        db_config.cnx.commit()

    def predict(self, feature):
        print(feature)
        scaledRain = list(map(lambda x: self.rainScaler.transform([[x]])[0][0], feature[:12]))
        scaledRiver = list(map(lambda x: self.riverScaler.transform([[x]])[0][0], feature[12:]))
        scaledFeature = scaledRain + scaledRiver
        prediction = self.model.predict(np.array([[scaledFeature]]))
        return self.riverScaler.inverse_transform(prediction)[0][0]
