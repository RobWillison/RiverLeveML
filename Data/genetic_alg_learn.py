
import numpy as np
from sklearn import linear_model
import pickle

trainingData = pickle.load(open("trainingData", "rb" ))[:200]
testingData = pickle.load(open("trainingData", "rb" ))[200:]
outputValuesTraining = pickle.load(open("outputValues", "rb" ))[:200]
outputValuesTesting = pickle.load(open("outputValues", "rb" ))[200:]

def write(x):
    model = linear_model.ElasticNet(l1_ratio=x[0])
    # Train the model using the training sets
    model.fit(trainingData, outputValuesTraining)
    pickle.dump( model, open( "classifier.clf", "wb" ))

def fitness(x):
    model = linear_model.ElasticNet(l1_ratio=x[0], max_iter=1000000)
    # Train the model using the training sets
    model.fit(trainingData, outputValuesTraining)
    return abs(model.score(testingData, outputValuesTesting))

def predictionError(weights, features):
    model = RobModel(weights)
    for feature in features:


bounds = [(0, 1)]
result = differential_evolution(fitness, bounds)
print(result.x, result.fun)

write(result.x)
