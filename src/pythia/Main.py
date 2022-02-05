import pandas as pd

from src.pythia.Constants import STRATEGY_SCHEMA, TYPE_ROW, TYPE_ATTRIBUTE, TYPE_FULL, MATCH_TYPE_CONTRADICTING, TYPE_FD, TYPE_FUNC
from src.pythia.DBUtils import getDBConnection
from src.pythia.Dataset import Dataset
from itertools import combinations, permutations
from src.pythia.Pythia import find_a_queries
from src.pythia.TemplateFactory import TemplateFactory

### DB CONNECTION POSTGRESQL
dialect = "postgresql"
user_uenc = "postgres"
pw_uenc = "postgres"
host = "localhost"
port = "5432"
dbname = "ambiguities"

if __name__ == '__main__':
    templateFactory = TemplateFactory()
    #file = "/Users/enzoveltri/Desktop/abalone.csv"
    file = "/Users/enzoveltri/Desktop/soccer.csv"
    d1 = Dataset(file, "soccer_new_new", 3, False, ["has","players"])
    cks = d1.getCompositeKeys()
    pk = d1.getPk()
    attributes = d1.getAttributes()
    normalizedAttributes = d1.getNormalizedAttributes()
    fds = d1.getFDs()
    dataframe = d1.getDataFrame()
    mappings = d1.getMappingsAttributes()
    print("Composite Keys: ", cks)
    print("Primary Keys: ", pk)
    print("Attributes: ", attributes)
    print("Norm Attributes:", normalizedAttributes)
    print("FDs: ", fds)
    #print(dataframe.head())
    checkDB = d1.storeInDB(user_uenc, pw_uenc, host, port, dbname)
    #print(checkDB)
    print("Mappings: ", mappings)
    d1.findAmbiguousAttributes(STRATEGY_SCHEMA)
    ambiguousAttributes = d1.getAmbiguousAttribute()
    print("Ambiguous attributes: ", ambiguousAttributes)
    ambiguousAttributesNormalized = d1.getAmbiguousAttributeNormalized()
    print("Ambiguous attributes norm: ", ambiguousAttributesNormalized)
    print("TEMPLATES: ", len(templateFactory.getTemplates()))
    rowsTemplates = templateFactory.getTemplatesByType(TYPE_ROW)
    attributeTemplates = templateFactory.getTemplatesByType(TYPE_ATTRIBUTE)
    fullTemplates = templateFactory.getTemplatesByType(TYPE_FULL)
    fdTemplates = templateFactory.getTemplatesByType(TYPE_FD)
    funcTemplates = templateFactory.getTemplatesByType(TYPE_FUNC)
    print(rowsTemplates)
    print(attributeTemplates)
    print(fullTemplates)
    print(fdTemplates)
    print(funcTemplates)
    connection = getDBConnection(user_uenc, pw_uenc, host, port, dbname)
    a_queries, a_queries_with_data = find_a_queries(d1, fullTemplates, MATCH_TYPE_CONTRADICTING, connection,
                   operators=["=", ">", "<"], functions=["min", "max"], executeQuery=True, limitQueryResults=10, shuffleQuery=True)
    print("Total A-Queries Generated:", len(a_queries))
    print("Differents A-Queries: ", len(set(a_queries)))
    for aq in a_queries:
        print(aq)


