import pandas as pd
from src.datasets.Utils import toSQLTable


class Covid:
    def __init__(self):
        self.file = 'data/all_cumsum.csv'

    def retrieve(self):
        self.df = pd.read_csv(self.file)
        self._updateData()

    def getDataframe(self):
        return self.df

    def _updateData(self):
        self.df = self.df.dropna()

    def toSQL(self):
        toSQLTable(self.df, "covid", "postgres", "postgres", "localhost", "5432", "ambiguities")