import pandas as pd
from src.datasets.Utils import toSQLTable

class Mushroom:
    def __init__(self):
        self.url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/mushroom/agaricus-lepiota.data'

    def retrieve(self):
        columns = ['class',
                   'cap-shape',
                   'cap-surface',
                   'cap-color',
                   'bruises?',
                   'odor',
                   'gill-attachment',
                   'gill-spacing',
                   'gill-size',
                   'gill-color',
                   'stalk-shape',
                   'stalk-root',
                   'stalk-surface-above-ring',
                   'stalk-surface-below-ring',
                   'stalk-color-above-ring',
                   'stalk-color-below-ring',
                   'veil-type',
                   'veil-color',
                   'ring-number',
                   'ring-type',
                   'spore-print-color',
                   'population',
                   'habitat']
        self.df = pd.read_csv(self.url, sep=',', names=columns)

    def getDataframe(self):
        return self.df

    def toSQL(self):
        toSQLTable(self.df,"mushroom", "postgres","postgres","localhost", "5432", "ambiguities")