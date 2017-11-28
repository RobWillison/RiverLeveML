import pickle
import db_config
from keras.models import load_model
import numpy as np

class RobModel:
    model = None
    rainScaler = None
    riverScaler = None
    riverId = None

    def __init__(self, riverId):
        self.riverId = riverId

    def set_model(self, model, rainScaler, riverScaler):
        self.model = model
        self.rainScaler = rainScaler
        self.riverScaler = riverScaler
        return self

    def load(self):
        cursor = db_config.cnx.cursor()
        sql = 'SELECT model FROM training_data WHERE river_id = %s'
        cursor.execute(sql, (self.riverId))
        model = pickle.loads(cursor.fetchone()['model'])
        self.model = load_model(model[2])
        self.rainScaler = model[0]
        self.riverScaler = model[1]
        return self


    def save(self):
        path = 'Data/Models/river' + str(self.riverId) + '.h5'
        self.model.save(path)
        model = [self.rainScaler, self.riverScaler, path]
        modelString = pickle.dumps(model)

        cursor = db_config.cnx.cursor()
        sql = 'UPDATE training_data SET model = %s WHERE river_id = %s'
        cursor.execute(sql, (modelString, self.riverId))
        db_config.cnx.commit()
        return self


    def predict(self, feature):
        print(feature)
        scaledRain = list(map(lambda x: self.rainScaler.transform([[x]])[0][0], feature[:12]))
        scaledRiver = list(map(lambda x: self.riverScaler.transform([[x]])[0][0], feature[12:]))
        scaledFeature = scaledRain + scaledRiver
        prediction = self.model.predict(np.array([[scaledFeature]]))
        return self.riverScaler.inverse_transform(prediction)[0][0]
