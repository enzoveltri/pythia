import pandas as pd
from src.datasets.Utils import toSQLTable

class Adult:
    def __init__(self, useFile):
        self.useFile = useFile
        if self.useFile:
            self.file = 'data/adult_micro.csv'
        else:
            self.url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data'

    def retrieve(self):
        if self.useFile:
            self.df = pd.read_csv(self.file)
            self._updateData()
        else:
            columns = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation',
                       'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week',
                       'native-country', 'salary']
            self.df = pd.read_csv(self.url, sep=',', header=None)
            self.df.columns = columns

    def getDataframe(self):
        return self.df

    def _updateData(self):
        self.df = self.df.fillna(0)
        self.df['incomeGT50'] = (self.df['income'] == " >50K")

    def toSQL(self):
        if self.useFile:
            toSQLTable(self.df,"adult_micro", "postgres","postgres","localhost", "5432", "ambiguities")
        else:
            toSQLTable(self.df, "adult", "postgres", "postgres", "localhost", "5432", "ambiguities")


