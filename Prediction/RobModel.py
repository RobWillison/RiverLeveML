import pickle
import db_config
import numpy as np

class RobModel:
    runOn = None
    runOff = None

    def __init__(self, runOn, runOff):
        self.runOn = runOn
        self.runOff = runOff

    # Feature [precip, level]
    def predict(self, feature):
        print(feature)
        
        if feature[0] == 0:
            print(self.runOff(feature[1]))
            return feature[1] + self.runOff(feature[1])

        upLevel = self.runOn(feature[0])

        return upLevel
