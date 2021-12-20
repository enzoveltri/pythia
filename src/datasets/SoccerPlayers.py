import requests
import pandas as pd
import json

from src.datasets.Utils import toSQLTable


class SoccerPlayers:
    def __init__(self):
        self.file = 'data/players.json'
        #self.baseURL = "https://www.whoscored.com/StatisticsFeed/1/GetPlayerStatistics?category=summary&subcategory=all&statsAccumulationType=0&isCurrent=true&playerId=&teamIds=&matchId=&stageId=&tournamentOptions=2,3,4,5,22&sortBy=Rating&sortAscending=&age=&ageComparisonType=&appearances=&appearancesComparisonType=&field=Overall&nationality=&positionOptions=&timeOfTheGameEnd=&timeOfTheGameStart=&isMinApp=false&page=1&includeZeroValues=&numberOfPlayersToPick="

    def retrieve(self):
        with open(self.file) as f:
            self.data = json.load(f)
        #url = self.baseURL + str(self.size)
        #r = requests.get(url=url)
        #self.data = r.json()
        playerList = self.data['playerTableStats']
        self.df = pd.DataFrame.from_records(playerList)

    def getDataframe(self):
        return self.df

    def toSQL(self):
        toSQLTable(self.df,"soccer", "postgres","postgres","localhost", "5432", "ambiguities")


