import json
import random
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

def getValueUniformFalse(valuesExclude, domain, maxAttempts = 10):
    attempts = 0
    domainList = list(domain)
    while True:
        elem = random.choice(domainList)
        attempts += 1
        if elem not in valuesExclude:
            return elem
        if attempts == maxAttempts:
            break
    # brute force
    for elem in domainList:
        if elem not in valuesExclude:
            return elem
    return None

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
def attr_amb(dataset, pysqldf, sentenceGenerator, operator, matchType, limit, limitPerType=-1):
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    counter = 0
    examples = []
    for attr1, attr2, label in ambiguousAttributes:
        #if (attr1.type == Constants.CATEGORICAL or attr2.type == Constants.CATEGORICAL and operator != "=" and operator != "<>"):
        #    continue
        #q = "SELECT " + pk.normalizedName + " as PK, " + attr1.normalizedName + " as AMB1, " + attr2.normalizedName + " as AMB2 FROM dataframe WHERE " + attr1.normalizedName + " " + operator + " " + attr2.normalizedName
        q = ""
        if matchType == Constants.MATCH_TYPE_CONTRADICTING:
            q = "SELECT d.'" + pk.normalizedName + "' as PK, d.'" + attr1.normalizedName + "' as AMB1, d.'" + attr2.normalizedName + "' as AMB2 FROM dataframe d WHERE d.'" + attr1.normalizedName + "' " + "<>" + " d.'" + attr2.normalizedName + "'"
        if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
            q = "SELECT d.'" + pk.normalizedName + "' as PK, d.'" + attr1.normalizedName + "' as AMB1, d.'" + attr2.normalizedName + "' as AMB2 FROM dataframe d WHERE d.'" + attr1.normalizedName + "' " + "=" + " d.'" + attr2.normalizedName + "'"
        if matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
            q = "SELECT d.'" + pk.normalizedName + "' as PK, d.'" + attr1.normalizedName + "' as AMB1, d.'" + attr2.normalizedName + "' as AMB2 FROM dataframe d WHERE d.'" + attr1.normalizedName + "' " + "<>" + " d.'" + attr2.normalizedName + "'"
        #q = "SELECT d.id as PK, d.field_goal_made as AMB1, d.3_point_field_goal_attempted as AMB2 FROM dataframe d WHERE d.field_goal_made = d.3_point_field_goal_attempted"
        #print("*** Q: ", q)
        dfSel = pysqldf(q).head(limit)
        domain1 = set(dataframe[attr1.normalizedName].unique())
        domain2 = set(dataframe[attr2.normalizedName].unique())
        #domain1 = set(pysqldf(q)["AMB1"].unique())
        #domain2 = set(pysqldf(q)["AMB2"].unique())
        domain = domain1.union(domain2)
        columnsDF = [pk, attr1, attr2]
        colSentenceDF = toColumnName(columnsDF)
        for index, row in dfSel.iterrows():
            dfSentence = row.to_frame().T
            dfSentence.columns = colSentenceDF
            pkCell = row["PK"]
            attr1Cell = row["AMB1"]
            attr2Cell = row["AMB2"]
            if matchType == Constants.MATCH_TYPE_CONTRADICTING:
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, attr1Cell, label)
                sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE, operator, matchType)
                examples.append(pythiaExample)
                counter += 1
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, attr2Cell, label)
                sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE, operator, matchType)
                examples.append(pythiaExample)
                counter += 1
            if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                ##attr1Cell or attr2Cell have the same value
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, attr2Cell, label)
                sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE, operator, matchType)
                examples.append(pythiaExample)
                counter += 1
            if matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                cell = getValueUniformFalse([attr1Cell, attr2Cell], domain)
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, cell, label)
                sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE, operator, matchType)
                examples.append(pythiaExample)
                counter += 1
            if limitPerType != -1 and counter >= limitPerType : return examples[0:limitPerType]
    return examples

#######################
## row ambiguity
#######################
def row_amb(dataset, pysqldf,sentenceGenerator, operator, matchType, limit, limitPerType=-1):
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
            q = "SELECT d1.'" + attr.normalizedName + "' as A1, d2.'" + attr.normalizedName + "' as A2, d1.'" + ckAttr.normalizedName + "' as CK_0_1, d2.'" + ckAttr.normalizedName + "' as CK_0_2"
            count = 1
            for other in othersAttr:
                q += ", d1.'" + other.normalizedName + "' as CK_" + str(
                    count) + "_1" + ", d2.'" + other.normalizedName + "' as CK_" + str(count) + "_2"
                count += 1
            last = othersAttr[-1]  ## ensure that at least one attribute is different
            q += " FROM dataframe d1, dataframe d2 WHERE d1." + ckAttr.normalizedName + " = d2." + ckAttr.normalizedName
            q += " AND d1.'" + last.normalizedName + "' <> d2.'" + last.normalizedName
            if matchType == Constants.MATCH_TYPE_CONTRADICTING or matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                q += "' AND d1.'" + attr.normalizedName + "' " + "<>" + " d2.'" + attr.normalizedName + "'"
            if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                q += " AND d1.'" + attr.normalizedName + "' " + "=" + " d2.'" + attr.normalizedName + "'"
            dfSel = pysqldf(q).head(limit)
            columnsDF = list()
            columnsDF.append(attr)
            columnsDF += ck
            colSentenceDF = toColumnName(columnsDF)
            domain = set(dataframe[attr.normalizedName].unique())
            #domain1 = set(dataframe["A1"].unique())
            #domain2 = set(dataframe["A2"].unique())
            #domain = domain1.union(domain2)
            for index, row in dfSel.iterrows():
                reshapedValues = row.values.reshape(len(columnsDF), 2).T
                dfSentence = pd.DataFrame(reshapedValues, columns=colSentenceDF)
                ckValue = row["CK_0_1"]
                # ckValue = row[ckAttr.normalizedName]
                attr1Value = row['A1']
                attr2Value = row['A2']
                if matchType == Constants.MATCH_TYPE_CONTRADICTING:
                    toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, attr1Value, attr.name)
                    sentence = sentenceGenerator.predict(toTotto)
                    pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW, operator, matchType)
                    examples.append(pythiaExample)
                    counter += 1
                    toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, attr2Value, attr.name)
                    sentence = sentenceGenerator.predict(toTotto)
                    pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW, operator, matchType)
                    examples.append(pythiaExample)
                    counter += 1
                if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                    toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, attr1Value,attr.name)
                    sentence = sentenceGenerator.predict(toTotto)
                    pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW, operator, matchType)
                    examples.append(pythiaExample)
                    counter += 1
                if matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                    cell = getValueUniformFalse([attr1Value, attr2Value], domain)
                    toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, cell, attr.name)
                    sentence = sentenceGenerator.predict(toTotto)
                    pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW, operator, matchType)
                    examples.append(pythiaExample)
                    counter += 1
                if limitPerType != -1 and counter >= limitPerType : return examples[0:limitPerType]
    return examples

#######################
## full ambiguity
#######################
def full_amb(dataset, pysqldf, sentenceGenerator, operator, matchType, limit, limitPerType=-1):
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    counter = 0
    examples = []
    for attr1, attr2, label in ambiguousAttributes:
        #if (attr1.type == Constants.CATEGORICAL and attr2.type == Constants.CATEGORICAL and operator != "=" and operator != "<>"):
        #    continue
        #print("Remove: ", attr1.normalizedName, attr2.normalizedName)
        for ck in cks:
            if attr1 not in ck and attr2 not in ck:
                ckAttr = ck[0]
                othersAttr = ck[1:]
                q = "SELECT b1.'" + ckAttr.normalizedName + "' as CK_0_1, b2.'" + ckAttr.normalizedName + "' as CK_0_2 "
                count = 1
                for other in othersAttr:
                    q += ", b1.'" + other.normalizedName + "' as CK_" + str(
                        count) + "_1" + ", b2.'" + other.normalizedName + "' as CK_" + str(count) + "_2"
                    count += 1
                q += ", b1.'" + attr1.normalizedName + "' as AMB_1_1, b2.'" + attr1.normalizedName + "' as AMB_1_2"
                q += ", b1.'" + attr2.normalizedName + "' as AMB_2_1, b2.'" + attr2.normalizedName + "' as AMB_2_2"
                q += " FROM dataframe b1, dataframe b2 WHERE b1.'" + ckAttr.normalizedName + "' = b2.'" + ckAttr.normalizedName
                last = othersAttr[-1] ## ensure that at least one attribute is different
                q += "' AND b1.'" + last.normalizedName + "' <> b2.'" + last.normalizedName
                if matchType == Constants.MATCH_TYPE_CONTRADICTING or matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                         q += "' AND b1.'" + attr1.normalizedName + "' <> b2.'" + attr1.normalizedName + "' AND b1.'" + attr2.normalizedName + "' <> b2.'" + attr2.normalizedName + "'"
                if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                        q += "' AND b1.'" + attr1.normalizedName + "' == b2.'" + attr1.normalizedName + "' AND b1.'" + attr2.normalizedName + "' == b2.'" + attr2.normalizedName \
                         + "' AND b1.'" + attr1.normalizedName + "' == b2.'" + attr2.normalizedName + "'"
                dfSel = pysqldf(q).head(limit)
                # print(q)
                # print(dfSel)
                domain1 = set(dataframe[attr1.normalizedName].unique())
                domain2 = set(dataframe[attr2.normalizedName].unique())
                domain = domain1.union(domain2)
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
                    if matchType == Constants.MATCH_TYPE_CONTRADICTING:
                        for attr in ["AMB_1_1", "AMB_1_2", "AMB_2_1", "AMB_2_2"]:
                            valueAmb = row[attr]
                            #print(subPKCell, ckAttr.name, valueAmb, label)
                            toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, subPKCell, ckAttr.name, valueAmb, label)
                            sentence = sentenceGenerator.predict(toTotto)
                            pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_FULL, operator, matchType)
                            examples.append(pythiaExample)
                            counter += 1
                    if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                        valueAmb = row["AMB_1_1"]
                        toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, subPKCell, ckAttr.name, valueAmb, label)
                        sentence = sentenceGenerator.predict(toTotto)
                        pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_FULL, operator, matchType)
                        examples.append(pythiaExample)
                        counter += 1
                    if matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                        ambValues = []
                        for attr in ["AMB_1_1", "AMB_1_2", "AMB_2_1", "AMB_2_2"]:
                            ambValues.append(row[attr])
                        valueAmb = getValueUniformFalse(ambValues, domain)
                        toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, subPKCell, ckAttr.name, valueAmb, label)
                        sentence = sentenceGenerator.predict(toTotto)
                        pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_FULL, operator, matchType)
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

    name = "soccer_small"
    #name = "adults"
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
    #sentenceGenerator = None
    limit = 10
    examplePerType = 15
    calculateAttribute = False
    calculateRow = True
    calculateFull = False
    #dataset.concept = "adults"
    #dataset.datasetName = "adults"
    dataset.concept = "soccer"
    dataset.datasetName = "soccer"
    #operators = ["<", ">"]
    operators = ["="]
    mt = Constants.MATCH_TYPE_CONTRADICTING
    #mt = Constants.MATCH_TYPE_UNIFORM_TRUE
    #mt = Constants.MATCH_TYPE_UNIFORM_FALSE
    mts = [Constants.MATCH_TYPE_CONTRADICTING, Constants.MATCH_TYPE_UNIFORM_TRUE, Constants.MATCH_TYPE_UNIFORM_FALSE]
    for op in operators:
        for mt in mts:
            if calculateAttribute:
                examples += attr_amb(dataset, pysqldf,sentenceGenerator, op, mt, limit, examplePerType)
            if calculateRow:
                examples += row_amb(dataset, pysqldf,sentenceGenerator, op, mt, limit, examplePerType)
            if calculateFull:
                examples += full_amb(dataset, pysqldf,sentenceGenerator, op, mt, limit, examplePerType)
    ## json export
    datasetJson = name + "_sentence.json"
    filePath = "/Users/enzoveltri/Downloads/datasets/"
    jsonFile = filePath + datasetJson
    with open(jsonFile, 'w') as outfile:
        for example in examples:
            outfile.write(example.toJSON()+"\n")
    print("Saved json: ", jsonFile)



