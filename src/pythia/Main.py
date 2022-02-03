import pandas as pd

from src.pythia.Constants import STRATEGY_SCHEMA
from src.pythia.Dataset import Dataset
from itertools import combinations, permutations

### DB CONNECTION POSTGRESQL
from src.pythia.TemplateFactory import TemplateFactory

dialect = "postgresql"
user_uenc = "postgres"
pw_uenc = "postgres"
host = "localhost"
port = "5432"
dbname = "ambiguities"

if __name__ == '__main__':
    templateFactory = TemplateFactory()
    file = "/Users/enzoveltri/Desktop/abalone.csv"
    d1 = Dataset(file, "abalone_new_new", 3)
    cks = d1.getCompositeKeys()
    pk = d1.getPk()
    attributes = d1.getAttributes()
    normalizedAttributes = d1.getNormalizedAttributes()
    fds = d1.getFDs()
    dataframe = d1.getDataFrame()
    mappings = d1.getMappingsAttributes()
    print(cks)
    print(pk)
    print(attributes)
    print(normalizedAttributes)
    print(fds)
    print(dataframe.head())
    #checkDB = d1.storeInDB(user_uenc, pw_uenc, host, port, dbname)
    #print(checkDB)
    print(mappings)
    d1.findAmbiguousAttributes(STRATEGY_SCHEMA)
    ambiguousAttributes = d1.getAmbiguousAttribute()
    print(ambiguousAttributes)
    ambiguousAttributesNormalized = d1.getAmbiguousAttributeNormalized()
    print(ambiguousAttributesNormalized)
    print("TEMPLATES: ", len(templateFactory.getTemplates()))

