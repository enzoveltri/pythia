import os
from pathlib import Path
import glob
import json
import pandas as pd
import random
import re
import xlsxwriter


from src.pythia.DBUtils import getEngine, getDBConnection, executeQueryBatch, getColumnsName
from src.pythia.PythiaExample import PythiaExample

SENTENCES_FILES = "/Users/enzoveltri/Downloads/datasets/annotation-exp/*.json"
TEMPLATE_FILES = "/Users/enzoveltri/Downloads/datasets/annotation-template/*.json"
EXCEL_FOLDER = "/Users/enzoveltri/Downloads/datasets/annotation-exp/"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ambiguities"
LIMIT_EXAMPLES = 10

def loadTemplates(file):
  f = open(file)
  data = json.load(f)
  examples = []
  for exampleForAQuery in data:
    for example in exampleForAQuery:
      dictData = {}
      for d in example['dataframe']:
        for key, value in d.items():
          dictData[key] = value
      selectedData = pd.DataFrame.from_dict(dictData)
      sentence = example['sentence']
      aquery = example["a_query"]
      exampleType = example["exampleType"]
      matchType = example["matchType"]
      example = PythiaExample(selectedData, sentence, aquery, None, exampleType, None, matchType)
      examples.append(example)
  return examples

def loadDataFromFile(file):
  f = open(file)
  data = json.load(f)
  examples = []
  for example in data:
      dictData = {}
      dictData = example['dataframe']
      selectedData = pd.DataFrame.from_dict(dictData)
      sentence = example['sentence']
      aquery = example["a_query"]
      exampleType = example["exampleType"]
      matchType = example["matchType"]
      example = PythiaExample(selectedData, sentence, aquery, None, exampleType, None, matchType)
      examples.append(example)
  return examples

def replaceTextBetween(originalText, delimeterA, delimterB, replacementText):
    leadingText = originalText.split(delimeterA)[0]
    trailingText = originalText.split(delimterB)[1]
    return leadingText + replacementText + trailingText

def getAmbAttributes(sql):
    result = re.search('"(.*)"', sql)
    pair = result.group(1)
    pair = pair.replace('"', '')
    pair = pair.replace("d.", '')
    pair = pair.replace("<>", ';')
    return pair

def getSQL(query, dbname, limit = -1):
    qMod = query
    qMod = qMod.replace("dataframe", dbname)
    qMod = replaceTextBetween(qMod, "SELECT", "FROM", "SELECT * FROM")
    qMod = qMod.replace("'", '"')
    if limit > 0:
        qMod += " LIMIT " + str(limit)
    return qMod

def getSQLForEvidente(dbname, attrID, valueID):
    return "SELECT * FROM " + dbname + ' d WHERE d."'+attrID+'" = ' + str(valueID)

def exportRandomExamples(examples, dbname, numAmb, numNotAmb, examplesTemplate, connection, limit=-1):
    ambExamples = []
    noAmbExamples = []
    templateExamples = []
    counterNumAmb = 0
    counterNumNoAmb = 0
    dataAmb = None
    addedSentences = set()
    while counterNumAmb < numAmb:
        rndEx = random.choice(examples)
        if (rndEx.matchType == 'contradicting'):
            sentence = rndEx.sentence
            query = rndEx.a_query
            dfQuery = pd.DataFrame.from_dict(rndEx.dataframe)
            id = dfQuery.iloc[0][0]
            idAttrName = dfQuery.columns[0]
            #print(sentence)
            #print(query)
            sqlQ = getSQL(query, dbname, limit)
            #print(sqlQ)
            ambAttrStr = getAmbAttributes(sqlQ)
            columns = getColumnsName(sqlQ, connection)
            results = executeQueryBatch(sqlQ, connection)
            queryEvidence = getSQLForEvidente(dbname, idAttrName, id)
            results += executeQueryBatch(queryEvidence, connection)
            #print(columns)
            #print(results)
            dfSample = pd.DataFrame(results,columns=columns)
            if dataAmb is None:
                dataAmb = dfSample
            else:
                dataAmb = dataAmb.append(dfSample)
            if sentence not in addedSentences:
                ambExamples.append((sentence, ambAttrStr))
                counterNumAmb += 1
                addedSentences.add(sentence)
    dataAmb = dataAmb.drop_duplicates()

    addedSentences = set()
    while counterNumNoAmb < numNotAmb:
        rndEx = random.choice(examples)
        if rndEx.matchType == 'NO_AMBIGUITY':
            sentence = rndEx.sentence
            if sentence not in addedSentences:
                ambAttrStr = getAmbAttributes(sqlQ)
                noAmbExamples.append((sentence, "NO"))
                counterNumNoAmb += 1
                addedSentences.add(sentence)

    counterNumAmb = 0
    addedSentences = set()

    while counterNumAmb < numAmb:
        rndEx = random.choice(examplesTemplate)
        if (rndEx.matchType == 'contradicting'):
            sentence = rndEx.sentence
            query = rndEx.a_query
            splits = query.split("AND")
            attr1 = splits[1].strip()
            attr2 = splits[2].strip()
            a1 = attr1.split(" ")[0].replace("b1.", "")
            a2 = attr2.split(" ")[0].replace("b1.", "")
            label = a1 + " ; " + a2
            if sentence not in addedSentences:
                templateExamples.append((sentence, label))
                counterNumAmb += 1
                addedSentences.add(sentence)

    return dataAmb, ambExamples, noAmbExamples, templateExamples

def printExamples(examplesAmb, examplesNoAmb, templateExamples, col, data, row, worksheet):
    columnsDF = data.columns
    for column in columnsDF:
        worksheet.write(row, col, column)
        col += 1
    row += 1
    for i, r in data.iterrows():
        col = 1
        for column in columnsDF:
            worksheet.write(row, col, r[column])
            col += 1
        row += 1
    row += 3
    col = 0  # reset column
    worksheet.write(row, 0, "Expected Label")
    worksheet.write(row, 1, "Sentence")
    row += 1
    for sentence, labelAmbAttr in examplesAmb:
        worksheet.write(row, col, labelAmbAttr)
        col += 1
        worksheet.write(row, col, sentence)
        row += 1
        col = 0
    for sentence, labelAmbAttr in examplesNoAmb:
        worksheet.write(row, col, labelAmbAttr)
        col += 1
        worksheet.write(row, col, sentence)
        row += 1
        col = 0
    for sentence, labelAmbAttr in templateExamples:
        worksheet.write(row, col, labelAmbAttr)
        col += 1
        worksheet.write(row, col, sentence)
        row += 1
        col = 0

def writeToExcel(dataAmb, ambExamples, noAmbExamples, templateExamples, worksheet):
    # Start from the first cell. Rows and columns are zero indexed.
    row = 0
    col = 1
    # Iterate over the data and write it out row by row.
    printExamples(ambExamples, noAmbExamples, templateExamples, col, dataAmb, row, worksheet)


if __name__ == '__main__':
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    random.seed(42)
    files = glob.glob(SENTENCES_FILES)
    files_template = glob.glob(TEMPLATE_FILES)
    files.sort()
    files_template.sort()
    fileExcel = EXCEL_FOLDER + "annotations.xlsx"
    workbook = xlsxwriter.Workbook(fileExcel)
    ambExamplesNum = 10
    noAmbExamplesNum = 10
    for file, templateFile in zip(files, files_template):
        print(file, templateFile)
        dbname = Path(file).name.replace("_sentence.json", "")
        examples = loadDataFromFile(file)
        examplesTemplate = loadTemplates(templateFile)
        dataAmb, ambExamples, noAmbExamples, templateExamples = exportRandomExamples(examples, dbname, ambExamplesNum, noAmbExamplesNum, examplesTemplate, connection, limit=LIMIT_EXAMPLES)
        worksheet = workbook.add_worksheet(dbname)
        writeToExcel(dataAmb, ambExamples, noAmbExamples, templateExamples, worksheet)
    workbook.close()


