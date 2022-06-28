import json
import random
import pandas as pd

from src.pythia import Constants
from src.pythia.DBUtils import getDBConnection, getEngine, getColumnsName
from src.pythia.Dataset import Dataset
from src.pythia.Pythia import toListCompositeKeys, find_a_queries
from src.pythia.PythiaExample import PythiaExample
from src.pythia.StringUtils import normalizeAscii
from src.pythia.T5Engine import T5Engine
from src.pythia.Constants import STRATEGY_SCHEMA, TYPE_ROW, TYPE_ATTRIBUTE, TYPE_FULL, MATCH_TYPE_CONTRADICTING, MATCH_TYPE_UNIFORM_TRUE
from munch import DefaultMunch
from pandasql import sqldf
import time

from src.pythia.T5SentenceGenerator import T5SentenceGenerator

### DB CONNECTION POSTGRESQL
from src.pythia.TemplateFactory import TemplateFactory

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
    #input += "<page_title> " + table + " </page_title> "
    #input += "<section_title> " + concept + " </section_title> "
    rowValue = "<row> <cell> " + str(pkValue) + " <col_header> " + str(pkAttributeName) + " </col_header> </cell> <cell> " + str(cellValue) + " <col_header> " + str(label) + " </col_header> </cell> </row>"
    input += "<table> " + rowValue + " </table>"
    return input

def tottoSentenceCellList(table, concept, cellList, attributeList):
    input = "totto table: "
    input += "<page_title> " + table + " </page_title> "
    input += "<section_title> " + concept + " </section_title> "
    rowValue = "<row>"
    for cell, attr in zip(cellList, attributeList):
        rowValue+="<cell> " + str(cell) + " <col_header> " + str(attr.name) + " <col_header> </cell> "
    rowValue += "</row>"
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
    #file = "/Users/enzoveltri/Downloads/datasets/adults.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/basket_full_names.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/basket_acronyms.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/basket_complete.csv"
    file = "/Users/enzoveltri/Downloads/datasets/mushroom.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/heart_2020.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/wineqt.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/superstore.csv"
    #file = "/Users/enzoveltri/Downloads/datasets/laptop.csv"
    #name = "soccer"
    #name = "soccer_small"
    #name = "iris"
    #name = "abalone"
    #name = "adults"
    #name = "basket_full_names"
    #name = "basket_acronyms"
    name = "mushroom"
    #name = "basket_complete_test_fd"
    #name = "heart_2020"
    #name = "wineqt"
    #name = "superstore"
    #name = "laptop"
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
    useCKs = False
    if len(cks) > 0:
        pk = cks[0]
        useCKs = True
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    examples = []
    for attr1, attr2, label in ambiguousAttributes:
        if (attr1.type != attr2.type): continue  ## compare only attributes of the same type
        q = ""
        if not useCKs:
            q += "SELECT d.'" + pk.normalizedName + "' as PK, d.'" + attr1.normalizedName + "' as AMB1, d.'" + attr2.normalizedName + "' as AMB2"
        else:
            q = "SELECT "
            count = 0
            for ckAttr in pk:
                q += "d.'"+ckAttr.normalizedName+"' as CK_" + str(count) + ", "
                count += 1
            q += "d.'" + attr1.normalizedName + "' as AMB1, d.'" + attr2.normalizedName + "' as AMB2"
        if matchType == Constants.MATCH_TYPE_CONTRADICTING:
            q += " FROM dataframe d WHERE d.'" + attr1.normalizedName + "' " + "<>" + " d.'" + attr2.normalizedName + "'"
        if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
            q += " FROM dataframe d WHERE d.'" + attr1.normalizedName + "' " + "=" + " d.'" + attr2.normalizedName + "'"
        if matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
            q += " FROM dataframe d WHERE d.'" + attr1.normalizedName + "' " + "<>" + " d.'" + attr2.normalizedName + "'"
        q += " ORDER BY random()"
        #print(q)
        dfSel = pysqldf(q).head(limit)
        #print(dfSel)
        domain1 = set(dataframe[attr1.normalizedName].unique())
        domain2 = set(dataframe[attr2.normalizedName].unique())
        domain = domain1.union(domain2)
        columnsDF = []
        if not useCKs:
            columnsDF = [pk, attr1, attr2]
        else :
            columnsDF += pk
            columnsDF += [attr1, attr2]
        colSentenceDF = toColumnName(columnsDF)
        counter = 0
        for index, row in dfSel.iterrows():
            dfSentence = row.to_frame().T
            dfSentence.columns = colSentenceDF
            pkCell = ""
            pkName = ""
            if not useCKs:
                pkCell = row["PK"]
                pkName = pk.name
            else:
                for i in range(0, len(pk)):
                    pkCell += str(row["CK_" + str(i)]) + " "
                    pkName += str(pk[i].name)+ " "
            attr1Cell = row["AMB1"]
            attr2Cell = row["AMB2"]
            if matchType == Constants.MATCH_TYPE_CONTRADICTING:
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pkName, attr1Cell, label)
                sentence = ""
                if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE, operator, matchType)
                examples.append(pythiaExample)
                counter += 1
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pkName, attr2Cell, label)
                if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE, operator, matchType)
                examples.append(pythiaExample)
                counter += 1
            if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                ##attr1Cell or attr2Cell have the same value
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, attr2Cell, label)
                sentence = ""
                if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE, operator, matchType)
                examples.append(pythiaExample)
                counter += 1
            if matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                cell = getValueUniformFalse([attr1Cell, attr2Cell], domain)
                toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, cell, label)
                sentence = ""
                if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ATTRIBUTE, operator, matchType)
                examples.append(pythiaExample)
                counter += 1
            if limitPerType != -1 and counter >= limitPerType : break
    return examples

#######################
## row ambiguity
#######################
def row_amb(dataset, pysqldf,sentenceGenerator, operator, matchType, limit, limitPerType=-1, useFDs=False):
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    examples = []
    fds = dataset.getFDs()
    if len(cks) == 0 and useFDs:
        cks = []
        for fd in fds:
            cks.append(fd[0])
    for ck in cks:
        attrNotCks = [x for x in attributes if x not in ck]
        if pk in attrNotCks: attrNotCks.remove(pk)
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
            q += " FROM dataframe d1, dataframe d2 WHERE d1.'" + ckAttr.normalizedName + "' = d2.'" + ckAttr.normalizedName
            q += "' AND d1.'" + last.normalizedName + "' <> d2.'" + last.normalizedName
            if matchType == Constants.MATCH_TYPE_CONTRADICTING or matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                q += "' AND d1.'" + attr.normalizedName + "' " + "<>" + " d2.'" + attr.normalizedName + "'"
            if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                q += "' AND d1.'" + attr.normalizedName + "' " + "=" + " d2.'" + attr.normalizedName + "'"
            q += " ORDER BY random()"
            dfSel = pysqldf(q).head(limit)
            columnsDF = list()
            columnsDF.append(attr)
            columnsDF += ck
            colSentenceDF = toColumnName(columnsDF)
            domain = set(dataframe[attr.normalizedName].unique())
            counter = 0
            for index, row in dfSel.iterrows():
                reshapedValues = row.values.reshape(len(columnsDF), 2).T
                dfSentence = pd.DataFrame(reshapedValues, columns=colSentenceDF)
                ckValue = row["CK_0_1"]
                attr1Value = row['A1']
                attr2Value = row['A2']
                if matchType == Constants.MATCH_TYPE_CONTRADICTING:
                    toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, attr1Value, attr.name)
                    sentence = ""
                    if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                    pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW, operator, matchType)
                    examples.append(pythiaExample)
                    counter += 1
                    toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, attr2Value, attr.name)
                    sentence = ""
                    if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                    pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW, operator, matchType)
                    examples.append(pythiaExample)
                    counter += 1
                if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                    toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, attr1Value,attr.name)
                    sentence = ""
                    if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                    pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW, operator, matchType)
                    examples.append(pythiaExample)
                    counter += 1
                if matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                    cell = getValueUniformFalse([attr1Value, attr2Value], domain)
                    toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, ckValue, ckAttr.name, cell, attr.name)
                    if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                    pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_ROW, operator, matchType)
                    examples.append(pythiaExample)
                    counter += 1
                if limitPerType != -1 and counter >= limitPerType : break
    return examples

#######################
## full ambiguity
#######################
def full_amb(dataset, pysqldf, sentenceGenerator, operator, matchType, limit, limitPerType=-1, useFDs=False):
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    dataframe = dataset.getDataFrame()
    examples = []
    fds = dataset.getFDs()
    if len(cks) == 0 and useFDs:
        cks = []
        for fd in fds:
            cks.append(fd[0])
    for attr1, attr2, label in ambiguousAttributes:
        if (attr1.type != attr2.type): continue  ## compare only attributes of the same type
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
                q += " ORDER BY random()"
                dfSel = pysqldf(q).head(limit)
                domain1 = set(dataframe[attr1.normalizedName].unique())
                domain2 = set(dataframe[attr2.normalizedName].unique())
                domain = domain1.union(domain2)
                columnsDF = list()
                columnsDF += ck
                columnsDF.append(attr1)
                columnsDF.append(attr2)
                colSentenceDF = toColumnName(columnsDF)
                counter = 0
                for index, row in dfSel.iterrows():
                    reshapedValues = row.values.reshape(len(columnsDF), 2).T
                    dfSentence = pd.DataFrame(reshapedValues, columns=colSentenceDF)
                    subPKCell = row["CK_0_1"]  ##could be also CK_0_2
                    if matchType == Constants.MATCH_TYPE_CONTRADICTING:
                        for attr in ["AMB_1_1", "AMB_1_2", "AMB_2_1", "AMB_2_2"]:
                            valueAmb = row[attr]
                            toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, subPKCell, ckAttr.name, valueAmb, label)
                            sentence = ""
                            if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                            pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_FULL, operator, matchType)
                            examples.append(pythiaExample)
                            counter += 1
                    if matchType == Constants.MATCH_TYPE_UNIFORM_TRUE:
                        valueAmb = row["AMB_1_1"]
                        toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, subPKCell, ckAttr.name, valueAmb, label)
                        sentence = ""
                        if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                        pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_FULL, operator, matchType)
                        examples.append(pythiaExample)
                        counter += 1
                    if matchType == Constants.MATCH_TYPE_UNIFORM_FALSE:
                        ambValues = []
                        for attr in ["AMB_1_1", "AMB_1_2", "AMB_2_1", "AMB_2_2"]:
                            ambValues.append(row[attr])
                        valueAmb = getValueUniformFalse(ambValues, domain)
                        toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, subPKCell, ckAttr.name, valueAmb, label)
                        sentence = ""
                        if sentenceGenerator is not None: sentence = sentenceGenerator.predict(toTotto)
                        pythiaExample = PythiaExample(dfSentence, sentence, q, toTotto, Constants.TYPE_FULL, operator, matchType)
                        examples.append(pythiaExample)
                        counter += 1
                    if limitPerType != -1 and counter >= limitPerType : break
    return examples

###################
## NO_AMB
###################
def no_amb(dataset, pysqldf, sentenceGenerator, operator, matchType, limit, limitPerType=-1):
    examples = []
    counter = 0
    ambiguousAttributes = dataset.getAmbiguousAttribute()
    pk = dataset.getPk()
    cks = dataset.getCompositeKeys()
    attributes = dataset.getAttributes()
    notAmbiguousAttr = dataset.findNonAmbiguousAttributes()
    notAmbiguousRow = [] ##TODO: implement rows
    for notAmb in notAmbiguousAttr:
        if notAmb == pk: continue
        counter = 0
        q = "SELECT d.'" + pk.normalizedName + "' as PK, d.'" + notAmb.normalizedName + "' as A FROM dataframe d ORDER BY random()"
        dfSel = pysqldf(q).head(limit)
        columnsDF = [pk, notAmb]
        colSentenceDF = toColumnName(columnsDF)
        for index, row in dfSel.iterrows():
            dfSentence = row.to_frame().T
            dfSentence.columns = colSentenceDF
            pkCell = row["PK"]
            attr1Cell = row["A"]
            toTotto = tottoSentence(dataset.getDatasetName(), dataset.concept, pkCell, pk.name, attr1Cell, notAmb.name)
            sentence = ""
            if sentenceGenerator is not None:
                sentence = sentenceGenerator.predict(toTotto)
            qMod = "SELECT " + pk.name + ", " + notAmb.name + " FROM dataframe WHERE " + notAmb.name + " = " + str(attr1Cell)
            pythiaExample = PythiaExample(dfSentence, sentence, qMod, toTotto, Constants.TYPE_ATTRIBUTE, operator, "NO_AMBIGUITY")
            examples.append(pythiaExample)
            counter += 1
            if limitPerType != -1 and counter >= limitPerType: break
    for ck in cks:
        qSel = "SELECT "
        counter_ck = 0
        for attr in ck:
            qSel += "d.'" + attr.normalizedName + "' as CK_" + str(counter_ck) +", "
            counter_ck += 1
        for notAmb in notAmbiguousAttr:
            if notAmb == pk: continue
            counter = 0
            q = qSel
            q += "d.'" + notAmb.normalizedName + "' as A FROM dataframe d"
            q += " ORDER BY random()"
            dfSel = pysqldf(q).head(limit)
            columnsDF = []
            columnsDF += ck
            columnsDF.append(notAmb)
            colSentenceDF = toColumnName(columnsDF)
            for index, row in dfSel.iterrows():
                dfSentence = row.to_frame().T
                dfSentence.columns = colSentenceDF
                cells = []
                for i in range(0, counter_ck):
                    cells.append(row["CK_"+str(i)])
                cells.append(row["A"])
                toTotto = tottoSentenceCellList(dataset.getDatasetName(), dataset.concept, cells, columnsDF)
                sentence = ""
                if sentenceGenerator is not None:
                    sentence = sentenceGenerator.predict(toTotto)
                qMod = "SELECT "
                where = "WHERE "
                for cell, attr in zip(cells, columnsDF):
                    qMod += attr.name+","
                    where += attr.name + " = " + str(cell) + " AND "
                qMod = qMod[:-1]
                where = where[:-5]
                qMod = qMod + " FROM dataframe " + where
                pythiaExample = PythiaExample(dfSentence, sentence, qMod, toTotto, Constants.TYPE_ATTRIBUTE, operator, "NO_AMBIGUITY")
                examples.append(pythiaExample)
                counter += 1
                if limitPerType != -1 and counter >= limitPerType: break
    return examples

if __name__ == '__main__': ## sentence generation
    DB_USER = "postgres"
    DB_PASSWORD = "postgres"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "ambiguities"

    name = "wineqt"
    #name = "heart_2020"
    datasetJson = name + ".json"
    filePath = "/Users/enzoveltri/Downloads/datasets/"
    jsonFile = filePath + datasetJson
    dict = dict()
    print("*** Load: ", jsonFile)
    with open(jsonFile) as json_file:
        dict = json.load(json_file)
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    datasetMunch = DefaultMunch.fromDict(dict, Dataset)
    dataset = Dataset(name, name)
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
    #dataframe = dataframe.rename(columns={"index": "id"})
    pysqldf = lambda q: sqldf(q, globals()) ## to query dataframes
    examples = []
    sentenceGenerator = T5SentenceGenerator()
    #sentenceGenerator = None
    limit = 1
    examplePerType = 1
    useFDs = False
    calculateAttribute = True
    calculateRow = False
    calculateFull = False
    calculateNoAmb = True
    #dataset.concept = "heart"
    #dataset.datasetName = "heart"
    dataset.concept = "wineqt"
    dataset.datasetName = "wineqt"
    #operators = ["<", ">"]
    operators = ["="]
    #mts = [Constants.MATCH_TYPE_CONTRADICTING, Constants.MATCH_TYPE_UNIFORM_TRUE, Constants.MATCH_TYPE_UNIFORM_FALSE]
    mts = [Constants.MATCH_TYPE_CONTRADICTING]
    start = time.time()
    for op in operators:
        for mt in mts:
            if calculateAttribute:
                print("*** Attribute")
                examples += attr_amb(dataset, pysqldf,sentenceGenerator, op, mt, limit, examplePerType)
                print("*** Total attribute:", len(examples))
            if calculateRow:
                print("*** Row")
                examples += row_amb(dataset, pysqldf,sentenceGenerator, op, mt, limit, examplePerType, useFDs)
                print("*** Total row:", len(examples))
            if calculateFull:
                print("*** Full")
                examples += full_amb(dataset, pysqldf,sentenceGenerator, op, mt, limit, examplePerType, useFDs)
                print("*** Total full:", len(examples))
        if calculateNoAmb:
            examples += no_amb(dataset, pysqldf,sentenceGenerator, op, mt, limit, examplePerType)
    print("Examples generated: ", len(examples))
    end = time.time()
    print("Time elapsed: ", (end - start))
    ## json export
    save = True
    if save:
        datasetJson = name + "_sentence.json"
        filePath = "/Users/enzoveltri/Downloads/datasets/"
        filePath = "/Users/enzoveltri/Downloads/datasets/annotation-exp/"
        jsonFile = filePath + datasetJson
        with open(jsonFile, 'w') as outfile:
            outfile.write("[")
            count = 1
            numExamples = len(examples)
            for example in examples:
                if count < numExamples:
                    outfile.write(example.toJSON()+"\n,\n")
                else:
                    outfile.write(example.toJSON() + "\n")
                count += 1
            outfile.write("]")
        print("Saved json: ", jsonFile)

#####################
## PYTHIA TEMPLATES
#####################

def to_df_attribute(row, cols):
    to_df = {}
    sentence = row[0]
    to_df[cols[1]] = [row[1], row[2]]
    to_df[cols[3]] = [row[3], row[4]]
    to_df[cols[5]] = [row[5], row[6]]
    return sentence, to_df

def to_df_row(row, cols):
    to_df = {}
    sentence = row[0]
    for i in range(1, len(cols), 2):
        to_df[cols[i]] = [row[i], row[i+1]]
    return sentence, to_df

def to_df_full(row, cols):
    to_df = {}
    sentence = row[0]
    for i in range(1, len(cols), 3):
        to_df[cols[i]] = [row[i], row[i+1], row[i+2]]
    return sentence, to_df

if __name__ == '__main__xx': ## sentence generation from Templates
    DB_USER = "postgres"
    DB_PASSWORD = "postgres"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "ambiguities"

    name = "mushroom"
    #name = "heart_2020"
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
    templateFactory = TemplateFactory()
    # pick default templates
    rowsTemplates = templateFactory.getTemplatesByType(TYPE_ROW)
    attributeTemplates = templateFactory.getTemplatesByType(TYPE_ATTRIBUTE)
    fullTemplates = templateFactory.getTemplatesByType(TYPE_FULL)
    #matches = [MATCH_TYPE_CONTRADICTING, MATCH_TYPE_UNIFORM_TRUE]
    matches = [MATCH_TYPE_CONTRADICTING]
    templates = [rowsTemplates, attributeTemplates, fullTemplates]
    examples = []
    for matchType in matches:
        for template in templates:
            a_queries, a_queries_with_data = find_a_queries(dataset, template, matchType, connection,
                                                            operators=["=", ">", "<"], functions=["min", "max"],
                                                            executeQuery=True, limitQueryResults=10, shuffleQuery=True)
            print("Total A-Queries Generated:", len(a_queries))
            print("Differents A-Queries: ", len(set(a_queries)))
            #for aq in a_queries:
            #    print(aq)
            for a_query, type, results, template_in_results, fd in a_queries_with_data:
                columnsQuery = getColumnsName(a_query, connection)
                for result in results:
                    sentence = ""
                    data = {}
                    if type == TYPE_ATTRIBUTE:
                        sentence, data = to_df_attribute(result, columnsQuery)
                    if type == TYPE_ROW:
                        sentence, data = to_df_row(result, columnsQuery)
                    if type == TYPE_FULL:
                        sentence, data = to_df_full(result, columnsQuery)
                    qMod = a_query.split("ORDER BY random()", 1)[0].replace("\n", " ")
                    pythiaExample = PythiaExample(pd.DataFrame.from_dict(data), sentence, qMod, "", type, ">", matchType)
                    examples.append(pythiaExample)
    ## json export
    datasetJson = name + "_template_sentence.json"
    filePath = "/Users/enzoveltri/Downloads/datasets/"
    jsonFile = filePath + datasetJson
    with open(jsonFile, 'w') as outfile:
        outfile.write("[")
        count = 1
        numExamples = len(examples)
        for example in examples:
            if count < numExamples:
                outfile.write(example.toJSON()+"\n,\n")
            else:
                outfile.write(example.toJSON() + "\n")
            count += 1
        outfile.write("]")
    print("Saved json: ", jsonFile)


