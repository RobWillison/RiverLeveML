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
        sql = 'SELECT path FROM models WHERE model_config_id = %s AND river_id = %s'
        cursor.execute(sql, (self.config_id, self.riverId))
        model = pickle.loads(cursor.fetchone()['model'])
        self.model = load_model(model[2])
        self.rainScaler = model[0]
        self.riverScaler = model[1]
        return self


    def save(self):
        path = 'Data/Models/river' + str(self.riverId) + '-' + str(self.configId) + '.h5'
        self.model.save(path)
        model = [self.rainScaler, self.riverScaler, path]
        modelString = pickle.dumps(model)

        self.add_model(path)

        cursor = db_config.cnx.cursor()
        sql = 'UPDATE training_data SET model = %s WHERE river_id = %s'
        cursor.execute(sql, (modelString, self.riverId))
        db_config.cnx.commit()
        return self

    def add_model(self, path):
        cursor = db_config.cnx.cursor()
        sql = 'REPLACE INTO models (river_id, model_config_id, model_path, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())'
        cursor.execute(sql, (self.riverId, self.configId, path))
        db_config.cnx.commit()

    def predict(self, feature):
        print(feature)
        scaledRain = list(map(lambda x: self.rainScaler.transform([[x]])[0][0], feature[:12]))
        scaledRiver = list(map(lambda x: self.riverScaler.transform([[x]])[0][0], feature[12:]))
        scaledFeature = scaledRain + scaledRiver
        prediction = self.model.predict(np.array([[scaledFeature]]))
        return self.riverScaler.inverse_transform(prediction)[0][0]
