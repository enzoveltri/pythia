import json
import pandas as pd

from src.pythia import Constants
from src.pythia.DBUtils import getDBConnection, getEngine
from src.pythia.Dataset import Dataset
from src.pythia.Pythia import toListCompositeKeys
from src.pythia.PythiaExample import PythiaExample
from src.pythia.StringUtils import normalizeAscii
from src.pythia.T5Engine import T5Engine
from src.pythia.Constants import STRATEGY_SCHEMA
from munch import DefaultMunch
from pandasql import sqldf
import time

from src.pythia.T5SentenceGenerator import T5SentenceGenerator

### DB CONNECTION POSTGRESQL
dialect = "postgresql"
user_uenc = "postgres"
pw_uenc = "postgres"
host = "localhost"
port = "5432"
dbname = "ambiguities"


def tottoSentence(table, concept,  pkValue, pkAttributeName,  cellValue, label):
    pkValue = normalizeAscii(str(pkValue))
    cellValue = normalizeAscii(str(cellValue))
    input = "totto table: "
    input += "<page_title> " + table + " </page_title> "
    input += "<section_title> " + concept + " </section_title> "
    rowValue = "<row> <cell> " + str(pkValue) + " <col_header> " + str(pkAttributeName) + " </col_header> </cell> <cell> " + str(cellValue) + " <col_header> " + str(label) + " </col_header> </cell> </row>"
    input += "<table> " + rowValue + " </table>"
    return input

def toColumnName(attrList):
    columnsName = []
    for attr in attrList:
        columnsName.append(attr.name)
    return columnsName

if __name__ == '__main__xx':  ## dataset profiling
    #file = "/Users/enzoveltri/Downloads/datasets/soccer.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/soccer_small.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/iris.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/abalone.csv"
    file = "/Users/enzoveltri/Downloads/datasets/adults.csv"
    #name = "soccer"
    #name = "soccer_small"
    #name = "iris"
    #name = "abalone"
    name = "adults"
    #################################
    ## 1 load data into Dataset obj
    #################################
    dataset = Dataset(name)
    dataset.initDataframe(file)
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
    dbEngine = getEngine(user_uenc, pw_uenc, host, port, dbname)
    checkDB = dataset.storeInDB(dbEngine)
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
    computeAmb = True
    if computeAmb:
        t5Engine = T5Engine()
        strategy = STRATEGY_SCHEMA
        dataset.findAmbiguousAttributes(strategy, t5Engine)
        ambiguousAttributes = dataset.getAmbiguousAttribute()
        print("### Ambiguous Pairs: ")
        for pair in ambiguousAttributes:
            print("Attr1: ", pair[0])
            print("Attr2: ", pair[1])
            print("Label: ", pair[2])
    print()
    # Using a JSON string
    datasetJson = name + ".json"
    filePath = "/Users/enzoveltri/Downloads/datasets/"
    jsonFile = filePath + datasetJson
    with open(jsonFile, 'w') as outfile:
        outfile.write(dataset.toJSON())
    print("Saved json: ", jsonFile)


#######################
## attribute ambiguity
#######################
def attr_amb(dataset, pysqldf, sentenceGenerator, limit, limitPerType=-1):
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    counter = 0
    # sentenceGenerator = T5SentenceGenerator()
    examples = []
    for attr1, attr2, label in ambiguousAttributes:
        # q = "SELECT id, "+ attr1.normalizedName + ", " + attr2.normalizedName + " FROM dataframe WHERE " + attr1.normalizedName + " > " + attr2.normalizedName
        q = "SELECT " + pk.normalizedName + " as PK, " + attr1.normalizedName + " as AMB1, " + attr2.normalizedName + " as AMB2 FROM dataframe WHERE " + attr1.normalizedName + " > " + attr2.normalizedName
        dfSel = pysqldf(q).head(limit)
        columnsDF = [pk, attr1, attr2]
        colSentenceDF = toColumnName(columnsDF)
        for index, row in dfSel.iterrows():
            dfSentence = row.to_frame().T
            dfSentence.columns = colSentenceDF
            pkCell = row["PK"]
            attr1Cell = row["AMB1"]
            attr2Cell = row["AMB2"]
            #toTotto = tottoSentence(dataset.getDatasetName(), pkCell, pk.name, attr1Cell, label)
            toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, attr1Cell, label)
            sentence = sentenceGenerator.predict(toTotto)
            pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE)
            examples.append(pythiaExample)
            counter += 1
            #toTotto = tottoSentence(dataset.getDatasetName(), pkCell, pk.name, attr2Cell, label)
            toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, attr2Cell, label)
            sentence = sentenceGenerator.predict(toTotto)
            pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE)
            examples.append(pythiaExample)
            counter += 1
            if limitPerType != -1 and counter >= limitPerType : return examples[0:limitPerType]
    return examples

#######################
## row ambiguity
#######################
def row_amb(dataset, pysqldf,sentenceGenerator, limit, limitPerType=-1):
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    counter = 0
    examples = []
    for ck in cks:
        attrNotCks = [x for x in attributes if x not in ck]
        attrNotCks.remove(pk)
        ckAttr = ck[0]
        othersAttr = ck[1:]
        for attr in attrNotCks:
            q = "SELECT d1." + attr.normalizedName + " as A1, d2." + attr.normalizedName + " as A2, d1." + ckAttr.normalizedName + " as CK_0_1, d2." + ckAttr.normalizedName + " as CK_0_2"
            count = 1
            for other in othersAttr:
                q += ", d1." + other.normalizedName + " as CK_" + str(
                    count) + "_1" + ", d2." + other.normalizedName + " as CK_" + str(count) + "_2"
                count += 1
            q += " FROM dataframe d1, dataframe d2 WHERE d1." + ckAttr.normalizedName + " = d2." + ckAttr.normalizedName + " and d1." + attr.normalizedName + " > d2." + attr.normalizedName
            dfSel = pysqldf(q).head(limit)
            columnsDF = list()
            columnsDF.append(attr)
            columnsDF += ck
            colSentenceDF = toColumnName(columnsDF)
            for index, row in dfSel.iterrows():
                reshapedValues = row.values.reshape(len(columnsDF), 2).T
                dfSentence = pd.DataFrame(reshapedValues, columns=colSentenceDF)
                #print(dfSentence)
                ckValue = row["CK_0_1"]
                # ckValue = row[ckAttr.normalizedName]
                attr1Value = row['A1']
                attr2Value = row['A2']
                # print(ckValue, attr1Value, attr2Value)
                #toTotto = tottoSentence(dataset.getDatasetName(), ckValue, ckAttr.name, attr1Value, attr.name)
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, attr1Value, attr.name)
                sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW)
                examples.append(pythiaExample)
                counter += 1
                #toTotto = tottoSentence(dataset.getDatasetName(), ckValue, ckAttr.name, attr2Value, attr.name)
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, attr1Value, attr.name)
                sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW)
                examples.append(pythiaExample)
                counter += 1
                if limitPerType != -1 and counter >= limitPerType : return examples[0:limitPerType]
    return examples

#######################
## full ambiguity
#######################
def full_amb(dataset, pysqldf, sentenceGenerator, limit, limitPerType=-1):
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    counter = 0
    examples = []
    for attr1, attr2, label in ambiguousAttributes:
        #print("Remove: ", attr1.normalizedName, attr2.normalizedName)
        for ck in cks:
            if attr1 not in ck and attr2 not in ck:
                ckAttr = ck[0]
                othersAttr = ck[1:]
                # q = "SELECT b1." + ckAttr.normalizedName + ", b2." + ckAttr.normalizedName + ", b3." + ckAttr.normalizedName
                # for other in othersAttr:
                #     q += ", b1." + other.normalizedName+ ", b2." + other.normalizedName + ", b3." + other.normalizedName
                # q += ", b1." + attr1.normalizedName + ", b2." + attr1.normalizedName + ", b3." + attr1.normalizedName
                # q += ", b1." + attr2.normalizedName + ", b2." + attr2.normalizedName + ", b3." + attr2.normalizedName
                # q += " FROM dataframe b1, dataframe b2, dataframe b3 WHERE b1."+ ckAttr.normalizedName + " = b2." + ckAttr.normalizedName \
                #      + " AND b2." + ckAttr.normalizedName + " = b3." + ckAttr.normalizedName \
                #      + " AND b1." + attr1.normalizedName + " > b2."+ attr1.normalizedName + " AND b1."+ attr2.normalizedName + " < b2." + attr2.normalizedName \
                #      + " AND b1." + attr1.normalizedName + " > b3." +attr1.normalizedName+ " AND b1."+ attr2.normalizedName + " < b3." + attr2.normalizedName
                ############################################
                q = "SELECT b1." + ckAttr.normalizedName + " as CK_0_1, b2." + ckAttr.normalizedName + " as CK_0_2 "
                count = 1
                for other in othersAttr:
                    q += ", b1." + other.normalizedName + " as CK_" + str(
                        count) + "_1" + ", b2." + other.normalizedName + " as CK_" + str(count) + "_2"
                    count += 1
                q += ", b1." + attr1.normalizedName + " as AMB_1_1, b2." + attr1.normalizedName + " as AMB_1_2"
                q += ", b1." + attr2.normalizedName + " as AMB_2_1, b2." + attr2.normalizedName + " as AMB_2_2"
                q += " FROM dataframe b1, dataframe b2 WHERE b1." + ckAttr.normalizedName + " = b2." + ckAttr.normalizedName \
                     + " AND b1." + attr1.normalizedName + " > b2." + attr1.normalizedName + " AND b1." + attr2.normalizedName + " < b2." + attr2.normalizedName
                dfSel = pysqldf(q).head(limit)
                # print(q)
                # print(dfSel)
                columnsDF = list()
                columnsDF += ck
                columnsDF.append(attr1)
                columnsDF.append(attr2)
                colSentenceDF = toColumnName(columnsDF)
                for index, row in dfSel.iterrows():
                    reshapedValues = row.values.reshape(len(columnsDF), 2).T
                    dfSentence = pd.DataFrame(reshapedValues, columns=colSentenceDF)
                    #print(dfSentence)
                    subPKCell = row["CK_0_1"]  ##could be also CK_0_2
                    for attr in ["AMB_1_1", "AMB_1_2", "AMB_2_1", "AMB_2_2"]:
                        valueAmb = row[attr]
                        #print(subPKCell, ckAttr.name, valueAmb, label)
                        #toTotto = tottoSentence(dataset.getDatasetName(), subPKCell, ckAttr.name, valueAmb, label)
                        toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, subPKCell, ckAttr.name, valueAmb, label)
                        sentence = sentenceGenerator.predict(toTotto)
                        pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_FULL)
                        examples.append(pythiaExample)
                        counter += 1
                        if limitPerType != -1 and counter >= limitPerType : return examples[0:limitPerType]
    return examples

if __name__ == '__main__': ## sentence generation
    DB_USER = "postgres"
    DB_PASSWORD = "postgres"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "ambiguities"

    #name = "soccer_small"
    name = "adults"
    datasetJson = name + ".json"
    filePath = "/Users/enzoveltri/Downloads/datasets/"
    jsonFile = filePath + datasetJson
    dict = dict()
    print("*** Load: ", jsonFile)
    with open(jsonFile) as json_file:
        dict = json.load(json_file)
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    datasetMunch = DefaultMunch.fromDict(dict, Dataset)
    dataset = Dataset(name)
    dataset.datasetName = datasetMunch.datasetName
    dataset.attributes = datasetMunch.attributes
    dataset.pk = datasetMunch.pk
    dataset.nameToAttribute = datasetMunch.nameToAttribute
    dataset.fds = datasetMunch.fds
    dataset.compositeKeys = datasetMunch.compositeKeys
    dataset.ambiguousAttribute = datasetMunch.ambiguousAttribute
    query = "select * from " + dataset.datasetName
    #dataset.dataframe = pd.read_sql(query, connection, index_col=dataset.pk.normalizedName)
    dataset.dataframe = pd.read_sql(query, connection)
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    #print(dataframe.head(2))
    #print(dataframe.columns)
    dataframe = dataframe.rename(columns={"index": "id"})
    pysqldf = lambda q: sqldf(q, globals()) ## to query dataframes
    examples = []
    sentenceGenerator = T5SentenceGenerator()
    limit = 5
    examplePerType = 30
    calculateAttribute = True
    calculateRow = True
    calculateFull = True
    dataset.concept = "adults"
    dataset.datasetName = "adults"
    if calculateAttribute:
        examples += attr_amb(dataset, pysqldf,sentenceGenerator, limit, examplePerType)
    if calculateRow:
        examples += row_amb(dataset, pysqldf,sentenceGenerator, limit, examplePerType)
    if calculateFull:
        examples += full_amb(dataset, pysqldf,sentenceGenerator, limit, examplePerType)
    ## json export
    datasetJson = name + "_sentence.json"
    filePath = "/Users/enzoveltri/Downloads/datasets/"
    jsonFile = filePath + datasetJson
    with open(jsonFile, 'w') as outfile:
        for example in examples:
            outfile.write(example.toJSON()+"\n")
    print("Saved json: ", jsonFile)



