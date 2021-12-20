import pandas as pd
from src.datasets.Utils import toSQLTable

class Iris:
    def __init__(self):
        self.url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data'

    def retrieve(self):
        columns = ['sepal length', 'sepal width', 'petal length', 'petal width', 'class']
        self.df = pd.read_csv(self.url, sep=',', names=columns)

    def getDataframe(self):
        return self.df

    def toSQL(self):
        toSQLTable(self.df,"iris", "postgres","postgres","localhost", "5432", "ambiguities")