from src.pythia.Constants import STRATEGY_SCHEMA, TYPE_ROW, TYPE_ATTRIBUTE, TYPE_FULL, TYPE_FD, TYPE_FUNC, \
    MATCH_TYPE_CONTRADICTING, MATCH_TYPE_UNIFORM_TRUE, MATCH_TYPE_UNIFORM_FALSE
from src.pythia.DBUtils import getDBConnection
from src.pythia.Dataset import Dataset
from src.pythia.Pythia import find_a_queries
from src.pythia.T5EngineMock import T5EngineMock
from src.pythia.TemplateFactory import TemplateFactory
from src.pythia.Profiling import getCompositeKeys, getFDs, getAmbiguousAttribute


### DB CONNECTION POSTGRESQL
dialect = "postgresql"
user_uenc = "postgres"
pw_uenc = "postgres"
host = "localhost"
port = "5432"
dbname = "ambiguities"

if __name__ == '__main__':
    #file = "/Users/enzoveltri/Downloads/datasets/soccer.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/iris.csv"
    file = "/Users/enzoveltri/Downloads/datasets/abalone.csv"
    #name = "soccer"
    name = "iris"
    name = "abalone"
    #################################
    ## 1 load data into Dataset obj
    #################################
    dataset = Dataset(file, name)
    attributes = dataset.getAttributes()
    print("### Attrs: ")
    for attr in attributes:
        print(attr)
    #################################
    ## 2 Find PK
    #################################
    dataset.findPK()
    pk = dataset.getPk()
    print("### PK: ")
    print(pk)
    #################################
    ## 3 Store in DB
    #################################
    df = dataset.getDataFrame()
    print(df.head(2))
    checkDB = dataset.storeInDB(user_uenc, pw_uenc, host, port, dbname)
    print("Stored in db: ", checkDB)
    ## TODO: store the dataset obj too as JSON (ignore unusefull informations)
    #################################
    ## 4 Composite Keys discovery
    #################################
    ## TODO: load dataset from DB and load df from SQL Table in Table
    computeCKs = True
    maxSizeKeys = 3  ## max attr number of the composite keys
    ckMinimal = False ## use only the minimal one i.e. the first one
    if computeCKs:
        dataset.findCompositeKeys(df, maxSizeKeys, ckMinimal)
        dataset.pruneCKsByType(useNumerical=False) ## set to True to use numerical attributes for CKs
        cks = dataset.getCompositeKeys()
        print("### CKs: ")
        for ck in cks:
            for attr in ck:
                print(attr)
            print("###########")
    #######################################
    ## 5 Functional Dependencies discovery
    #######################################
    ## TODO: load df from SQL Table
    computeFDs = True
    if computeFDs:
        dataset.findFDs(df, True)
        fds = dataset.getFDs()
        print("### FDs: ")
        for fd in fds:
            for attr in fd:
                print(attr)
            print("###########")
    ## TODO: add rowMeaning to FDs with default values
    rowMeaning = ["player", "in"]
    dataset.extendFDs(rowMeaning)
    ###################################
    ## 6 Ambiguous Attribute discovery
    ################################### 
    t5Engine = T5EngineMock()
    strategy = STRATEGY_SCHEMA
    dataset.findAmbiguousAttributes(strategy, t5Engine)
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    print("### Ambiguous Pairs: ")
    for pair in ambiguousAttributes:
        print("Attr1: ", pair[0])
        print("Attr2: ", pair[1])
        print("Label: ", pair[2])
    ###########################
    ## 7 Templates definition
    ###########################
    templateFactory = TemplateFactory()
    # pick default templates
    rowsTemplates = templateFactory.getTemplatesByType(TYPE_ROW)
    attributeTemplates = templateFactory.getTemplatesByType(TYPE_ATTRIBUTE)
    fullTemplates = templateFactory.getTemplatesByType(TYPE_FULL)
    fdTemplates = templateFactory.getTemplatesByType(TYPE_FD)
    funcTemplates = templateFactory.getTemplatesByType(TYPE_FUNC)
    #print(rowsTemplates) ## OK
    #print(attributeTemplates) ## OK
    #print(fullTemplates) ## OK
    #print(fdTemplates) ## OK TODO: need a way to write labels
    #print(funcTemplates) ## OK
    connection = getDBConnection(user_uenc, pw_uenc, host, port, dbname)
    a_queries, a_queries_with_data = find_a_queries(dataset, attributeTemplates, MATCH_TYPE_CONTRADICTING, connection,
                                                    operators=["=", ">", "<"], functions=["min", "max"],
                                                    executeQuery=True, limitQueryResults=10, shuffleQuery=True)
    print("Total A-Queries Generated:", len(a_queries))
    print("Differents A-Queries: ", len(set(a_queries)))
    for aq in a_queries:
        print(aq)

