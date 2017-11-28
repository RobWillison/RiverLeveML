import pickle
import random
from Prediction.RobModel import RobModel
import numpy as np
import db_config
from matplotlib import pyplot
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from math import sqrt
from numpy import concatenate
from sklearn.metrics import mean_squared_error

np.seterr(divide='ignore', invalid='ignore')

def loadData(riverId):
    cursor = db_config.cnx.cursor()
    sql = 'SELECT data FROM training_data WHERE river_id = %s'
    cursor.execute(sql, (riverId))
    return pickle.loads(cursor.fetchone()['data'])

def train(riverId):
    data = loadData(riverId)
    data_X = data[0]
    data_y = data[1]
    rainScaler = data[2]
    riverScaler = data[3]

    rng_state = np.random.get_state()
    np.random.shuffle(data_X)
    np.random.set_state(rng_state)
    np.random.shuffle(data_y)

    train_X = np.array(data_X[:150])
    train_y = np.array(data_y[:150])

    test_X = np.array(data_X[150:])
    test_y = np.array(data_y[150:])

    # design network
    model = Sequential()
    model.add(LSTM(750, input_shape=(train_X.shape[1], train_X.shape[2])))
    model.add(Dense(1))
    model.compile(loss='mae', optimizer='adam')

    # fit network
    history = model.fit(train_X, train_y, epochs=200, batch_size=72, validation_data=(test_X, test_y), verbose=0, shuffle=False)
    # plot history
    # make a prediction
    yhat = model.predict(test_X)
    test_X = test_X.reshape((test_X.shape[0], test_X.shape[2]))
    # invert scaling for forecast
    inv_yhat = concatenate((yhat, test_X[:, 1:]), axis=1)
    inv_yhat = inv_yhat[:,0]
    # invert scaling for actual
    test_y = test_y.reshape((len(test_y), 1))
    inv_y = concatenate((test_y, test_X[:, 1:]), axis=1)
    inv_y = inv_y[:,0]
    # calculate RMSE
    rmse = sqrt(mean_squared_error(inv_y, inv_yhat))
    print('Test RMSE: %.3f' % rmse)

    robModel = RobModel(riverId).set_model(model, rainScaler, riverScaler).save()


def getModel(riverId):
    return RobModel(riverId).load()
