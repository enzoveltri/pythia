import pandas as pd
from src.datasets.Utils import toSQLTable
import uci_dataset as uci_dataset

class Abalone:
    def __init__(self):
        self.datasetName= "abalone"

    def retrieve(self):
        self.df = uci_dataset.load_abalone()

    def getDataframe(self):
        return self.df

    def toSQL(self):
        toSQLTable(self.df,"abalone", "postgres","postgres","localhost", "5432", "ambiguities")