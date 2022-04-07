import json

class PythiaExample:

    def __init__(self, dataframe, sentence, a_query, totto, exampleType, operator, matchType):
        #self.dataframe = dataframe.to_json()
        self.dataframe = dataframe.to_dict(orient='list')
        self.sentence = sentence
        self.a_query = a_query
        self.totto = totto
        self.exampleType = exampleType
        self.operator = operator
        self.matchType = matchType

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)