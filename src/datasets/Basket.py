import pandas as pd
from src.datasets.Utils import toSQLTable

class Basket:
    def __init__(self):
        self.file = 'data/basket.csv'

    def retrieve(self):
        self.df = pd.read_csv(self.file)
        self._updateData()

    def getDataframe(self):
        return self.df

    def _updateData(self):
        ## make Name attribute
        tmp = self.df["Starters"].str.split(" ", n=1, expand=True)
        self.df['Name'] = tmp[0]
        self.df['Surname'] = tmp[1]
        self.df = self.df.fillna(0)

    def toSQL(self):
        toSQLTable(self.df,"basket", "postgres","postgres","localhost", "5432", "ambiguities")