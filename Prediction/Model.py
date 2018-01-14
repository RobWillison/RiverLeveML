import pickle
import random
from Prediction.RobModel import RobModel
import numpy as np
import db_config
from matplotlib import pyplot
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.optimizers import Adam
from sklearn.feature_selection import RFE
from sklearn.linear_model import LogisticRegression
from math import sqrt
from numpy import concatenate
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import numpy

np.seterr(divide='ignore', invalid='ignore')

def loadData(riverId):
    cursor = db_config.cnx.cursor()
    sql = 'SELECT data FROM training_data WHERE river_id = %s'
    cursor.execute(sql, (riverId))
    return pickle.loads(cursor.fetchone()['data'])

def train(riverId, configId=1, data=None):
    config = getConfig(configId)

    if data == None:
        data = loadData(riverId)
    print(len(data[0]))
    data_X = data[0]
    data_y = data[1]
    print(np.array(data_X).shape)
    rng_state = np.random.get_state()
    np.random.shuffle(data_X)
    np.random.set_state(rng_state)
    np.random.shuffle(data_y)

    split = int(len(data_X) * 0.8)
    train_X = np.array(data_X[:split])
    train_y = np.array(data_y[:split])

    test_X = np.array(data_X[split:])
    test_y = np.array(data_y[split:])

    # design network
    model = Sequential()
    print(train_X.shape)
    model.add(LSTM(200, input_shape=(train_X.shape[1], train_X.shape[2])))
    model.add(Dense(1))
    model.compile(loss='mae', optimizer='adam', metrics=['mae', 'acc'])

    # fit network
    history = model.fit(train_X, train_y, epochs=500, batch_size=72, validation_data=(test_X, test_y), verbose=1, shuffle=True)
    # plot history
    # plot history
    pyplot.plot(history.history['loss'], label='train')
    pyplot.plot(history.history['val_loss'], label='test')
    pyplot.legend()
    pyplot.show()
    # make a prediction
    yhat = model.predict(test_X)

    robModel = RobModel(riverId, configId).set_model(model).save()
    return robModel

def getConfig(id):
    cursor = db_config.cnx.cursor()
    sql = 'SELECT * FROM model_configs WHERE id = %s'
    cursor.execute(sql, (id))
    return cursor.fetchone()


def getModel(riverId, configId=1):
    return RobModel(riverId, configId).load()
