import json

class PythiaExample:

    def __init__(self, dataframe, sentence, a_query, totto, exampleType):
        #self.dataframe = dataframe.to_json()
        self.dataframe = dataframe.to_dict(orient='list')
        self.sentence = sentence
        self.a_query = a_query
        self.totto = totto
        self.exampleType = exampleType

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)