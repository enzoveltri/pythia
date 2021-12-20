import pandas as pd
import glob
from src.datasets.Utils import toSQLTable

class BasketComplete:
    def __init__(self, fullNames):
        self.basePath = 'data/basket_tables/'
        self.cityTeamsTowns = 'data/basket_tables/CityTeam.xlsx'
        self.fullNames = fullNames

    def retrieve(self):
        fileNames = glob.glob(self.basePath + "player*.csv")
        dfFiles = []
        for file in fileNames:
            dfFiles.append(pd.read_csv(file))
        dfCityTeamsTown = pd.read_excel(self.cityTeamsTowns)
        dfMerged = []
        for df in dfFiles:
            dfMerge = df.merge(dfCityTeamsTown, left_on='TEAM_CITY', right_on='City')
            dfMerge = dfMerge.drop(columns=['City'])
            dfMerged.append(dfMerge)
        self.df = pd.concat(dfMerged)
        self._updateData()
        if self.fullNames:
            self.df.columns = self.df.columns.str.lower()
            self.df.rename(columns=self.mappingColumnsName(), inplace=True)

    def mappingColumnsName(self):
        mappings = {'id': 'id',
                    'player_name': 'player_name',
                    'first_name': 'first_name',
                    'min': 'minutes',
                    'fgm': 'field goal made',
                    'reb': 'rebounds',
                    'fg3a': '3 point field goal attempted',
                    'ast': 'assists',
                    'fg3m': '3 point field goal made',
                    'oreb': 'offensive rebounds',
                    'to': 'turnovers',
                    'start_position': 'start_position',
                    'pf': 'personal fouls',
                    'pts': 'points',
                    'fga': 'field goal attempts',
                    'stl': 'steals',
                    'fta': 'free throws attempted',
                    'blk': 'blocks',
                    'dreb': 'defensive rebounds',
                    'ftm': 'free throws made',
                    'ft_pct': 'free throws percentage',
                    'fg_pct': 'field goals percentage',
                    'fg3_pct': '3 point field goal percentage',
                    'second_name': 'second_name',
                    'team_city': 'team_city',
                    'team': 'team',
                    'town': 'town'}
        return mappings

    def getDataframe(self):
        return self.df

    def _updateData(self):
        self.df = self.df.dropna()

    def toSQL(self):
        if self.fullNames:
            toSQLTable(self.df, "basket_full_names", "postgres", "postgres", "localhost", "5432", "ambiguities")
        else:
            toSQLTable(self.df,"basket_acronyms", "postgres","postgres","localhost","5432","ambiguities")