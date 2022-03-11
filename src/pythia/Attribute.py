from src.pythia.StringUtils import normalizeString
import json

class Attribute:

    def __init__(self, name):
        self.name = name
        self.normalizedName = normalizeString(name, "_")
        self.type = None

    def setType(self, type):
        self.type = type

    def getName(self):
        return self.name

    def getNormalizedName(self):
        return self.normalizedName

    def getType(self):
        return self.type

    def __str__(self):
        return "Name: " + self.name + " Normalized: " + self.normalizedName + " Type: " + self.type

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,  indent=4)

