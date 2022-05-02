import csv
import json
import os
import random

from fastapi import APIRouter, Depends, UploadFile, Form

from src.pythia.Constants import *
from src.pythia.ExportResult import ExportResult
from src.pythia.Pythia import find_a_queries
from src.pythia.T5Engine import T5Engine
from src.pythia.TemplateFactory import TemplateFactory, getTemplatesByName, getOperatorsFromTemplate
from src.rest.Authentication import User, get_current_active_user
from src.pythia.DBUtils import getScenarioListFromDb, getScenarioFromDb, deleteScenarioFromDb, existsDTModelInDb, \
    insertScenario, getEngine, updateScenario, updateTemplates, getTemplatesFromDb, getDBConnection, getColumnsName, \
    getDbUser, getDbPassword, getDbHost, getDbPort, getDbName, executeQueryBatch
from src.pythia.Dataset import Dataset

t5Engine = T5Engine().getInstance()

pythiaroute = APIRouter()

@pythiaroute.get("/get")
def getScenarios(user: User = Depends(get_current_active_user)):
    list = getScenarioListFromDb(user.username)
    result = {}
    for row in list:
        result[row[0]] = row[0].replace(user.username + "_", "")
    return result

@pythiaroute.get("/check/{name}")
def checkScenario(name: str, user: User = Depends(get_current_active_user)):
    name = name.replace(".csv", "")
    name = user.username + "_" + name
    result = existsDTModelInDb(name)
    return result


@pythiaroute.post("/create")
def uploadFile(file: UploadFile = Form(...), user: User = Depends(get_current_active_user)):
    tmpFolder = "../../data/tmp/" ## TODO: use system tmp folder
    if not os.path.exists(tmpFolder): os.makedirs(tmpFolder)
    file_location = f"../../data/tmp/{file.filename}" ## TODO: user the reference to the folder above
    name = file.filename.replace(".csv", "")
    datasetName = user.username + "_" + name
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    dataset = Dataset(datasetName)
    dataset.initDataframe(file_location)
    attributes = dataset.getAttributes()
    df = dataset.getDataFrame()
    dataset.findPK()
    engine = getEngine(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    checkDB = dataset.storeInDB(engine)
    print("*** Check DB: ", checkDB)
    insertScenario(datasetName, user.username, dataset)
    templateFactory = TemplateFactory()
    templates = templateFactory.templatesToJson()
    updateTemplates(datasetName, templates)
    os.remove(file_location)
    return datasetName


@pythiaroute.get("/dataframe/{name}/{offset}/{limit}")
def getDataframe(name: str, offset: int, limit: int, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    return scenario.dataframe.iloc[offset:limit].to_html(classes='table table-striped table-bordered table-sm')

@pythiaroute.get("/dataframe/count/{name}")
def getDataframeCount(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    return len(scenario.dataframe)

@pythiaroute.delete("/delete/{name}")
def deleteScenario(name: str, user: User = Depends(get_current_active_user)):
    deleteScenarioFromDb(name)


@pythiaroute.get("/get/{name}")
def getScenario(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    return scenario.toJSON()

@pythiaroute.get("/get/templates/{name}")
def getTemplates(name: str, user: User = Depends(get_current_active_user)):
    templates = getTemplatesFromDb(name)
    return json.dumps(templates)

@pythiaroute.get("/get/templates/structure/{type}")
def getTemplateStructure(type: str, user: User = Depends(get_current_active_user)):
    result = []
    templateFactory = TemplateFactory()
    templateStruct = templateFactory.getTemplatesByType(type)
    if len(templateStruct) > 0:
        for template, templateType, printF, name in templateStruct:
            result.append((template, templateType, [], ""))
    else:
        result.append(("", "", "", ""))
    return json.dumps(result[0])

@pythiaroute.get("/find/pk/{name}")
def findPrimaryKey(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    scenario.findPK()
    updateScenario(name, scenario)
    return scenario.toJSON()

@pythiaroute.get("/find/cks/{name}")
def findCompositeKeys(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    maxSizeKeys = 3  ## TODO: parameters
    ckMinimal = False  ## TODO: parameters
    useNumerical = False ## TODO: parameters
    scenario.findCompositeKeys(scenario.dataframe, maxSizeKeys, ckMinimal)
    scenario.pruneCKsByType(useNumerical)
    updateScenario(name, scenario)
    return scenario.toJSON()

@pythiaroute.get("/find/fds/{name}")
def findFDs(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    scenario.findFDs(scenario.dataframe, True)
    fds = scenario.getFDs()
    if len(fds) > 0:
        rowMeaning = ["has", "in"] ## TODO: parameter for the next function
        scenario.extendFDs(rowMeaning)
        updateScenario(name, scenario)
    return scenario.toJSON()

@pythiaroute.get("/find/ambiguous/{name}")
def findAmbiguous(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    strategy = STRATEGY_SCHEMA ## TODO: Parameter
    scenario.findAmbiguousAttributes(strategy, t5Engine)
    ambiguousAttributes = scenario.getAmbiguousAttribute()
    updateScenario(name, scenario)
    return scenario.toJSON()


@pythiaroute.post("/save/{name}")
def saveScenario(name: str,  scenario = Form(...), user: User = Depends(get_current_active_user)):
    updateScenario(name, scenario, False)
    scenario = getScenarioFromDb(name)
    return scenario.toJSON()

@pythiaroute.post("/save/templates/{name}")
def saveTemplates(name: str,  templates = Form(...), user: User = Depends(get_current_active_user)):
    templates = templates.replace("''", "'").replace("'", "''")
    updateTemplates(name, templates)
    templates = getTemplatesFromDb(name)
    return json.dumps(templates)


@pythiaroute.post("/predict/{name}")
def predict(name: str, strategy: str = Form(...), structure: str = Form(...), limitResults: int = Form(...), user: User = Depends(get_current_active_user)):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    scenario = getScenarioFromDb(name)
    totalTemplates = getTemplatesFromDb(name)
    print("*** Schema: ", STRATEGY_SCHEMA)
    print("*** Strategy: ", strategy)
    print("*** Structure: ", structure)
    templates = getTemplatesByName(totalTemplates, structure)
    operators = getOperatorsFromTemplate(templates[0])
    print("*** Operators: ", operators)
    a_queries, a_queries_with_data = find_a_queries(scenario, templates, strategy, connection,
                                                    operators, functions=["min", "max"],
                                                    executeQuery=True, limitQueryResults=limitResults, shuffleQuery=True)
    print("*** Total A-Queries Generated:", len(a_queries))
    print("*** Differents A-Queries: ", len(set(a_queries)))

    result = []
    tName = scenario.datasetName
    index = 0
    for a_query, type, results, template, fd in a_queries_with_data:
        tables_from_sentences = []
        dict_from_sentences = []
        to_totto = []
        if (type == TYPE_FD):
            pk = scenario.pk.name
            # TODO: to_totto_fd raises an exception. See the method for details
            # fdTemplateQuery = "SELECT b1.$LHS$,b1.$RHS$, b1.$PK$ FROM $TABLE$ b1 WHERE b1.$RHS$ = $VALUE$"
            # to_totto = to_totto_fd(results, fdTemplateQuery, tName, pk, connection, fd)
            # tables_from_sentences = get_tables_from_sentences(to_totto)
        if (type == TYPE_ROW):
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_row(results, columnsQuery)
            tables_from_sentences = get_tables_from_sentences(to_totto)
            dict_from_sentences = get_dict_from_sentences(to_totto)
        if (type == TYPE_ATTRIBUTE):
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_row(results, columnsQuery)
            tables_from_sentences = get_tables_from_sentences(to_totto)
            dict_from_sentences = get_dict_from_sentences(to_totto)
        if (type == TYPE_FULL):
            # TODO: to_totto_full raises an exception. See the method for details
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_full(results, columnsQuery)
            tables_from_sentences = get_tables_from_sentences(to_totto)
            dict_from_sentences = get_dict_from_sentences(to_totto)
        export_results = []
        for i in range(len(results)):
            if len(dict_from_sentences) > 0:  # TODO: remove if clause when all the to_totto are ok
                export_results.append(ExportResult(results[i][0], a_query, template[1], strategy, dict_from_sentences[i]))
            # export_results.append(ExportResult(results[i][0], a_query, template[1], strategy, dict_from_sentences[i]))
        result.append((index, a_query, type, results, template, fd, tables_from_sentences, export_results))
        index += 1
    return result


#TODO: Move next methods to appropriate class
def get_tables_from_sentences(to_totto_list):
    totto_examples = []
    for sentence, data in to_totto_list:
        example = to_sentence_details_table(data)
        totto_examples.append(example)
    return totto_examples


def get_dict_from_sentences(to_totto_list):
    dict_examples = []
    for sentence, data in to_totto_list:
        example = to_sentence_details_dict(data)
        dict_examples.append(example)
    return dict_examples


def to_sentence_details_table(selectedData):
    table = '<table class="table table-sm table-striped table-bordered table-hover">'
    table += '<thead><tr>'
    columns = selectedData[0]
    for pos in range(0, len(columns)):
        table += '<th>' + str(columns[pos]) + '</th>'
    table += '</tr></thead>'
    table += '<tbody>'
    for row in selectedData[1:]:
        table += '<tr>'
        for pos in range(0, len(columns)):
            table += "<td> " + str(row[pos]) +"</td>"
        table += '</tr>'
    table += '</tbody></table>'
    return table

def to_sentence_details_dict(selectedData):
    rows = []
    columns = selectedData[0]
    for pos in range(0, len(columns)):
        dict = ()
        data = []
        for row in selectedData[1:]:
            data.append(row[pos])
        rows.append({columns[pos]: data})
    return rows

def to_totto_full(results, cols):
    to_totto = []
    for row in results:
        sentence = row[0]
        colList = []
        row1List = []
        row2List = []
        for i in range(1, len(cols), 2):
            colList.append(cols[i])
            row1List.append(row[i])
            #row2List.append(row[i+1]) #TODO: row[i+1] raises an exception in some rows. Temporarily I put an if in the next line
            if len(row) > (i+1):
                row2 = row[i + 1]
            else:
                row2 = "OVERSIZE"
            row2List.append(row2)
        data = [tuple(colList), tuple(row1List), tuple(row2List)]
        to_totto.append((sentence, data))
    return to_totto


def to_totto_row(results, cols):
    to_totto = []
    for row in results:
        sentence = row[0]
        total = len(row)
        i = 1
        t0 = []
        t1 = []
        t2 = []
        while i < total:
            t0.append(cols[i])
            t1.append(row[i])
            t2.append(row[i + 1])
            i += 2
        t0 = tuple(t0)
        t1 = tuple(t1)
        t2 = tuple(t2)
        data = [t0, t1, t2]
        to_totto.append((sentence, data))
    return to_totto

def to_totto_fd(results, fdTemplateQuery, tableName , pk, connection, fd):
    to_totto = []
    print("*** fd: ", fd)
    lhsAttr = fd[0][0]
    rhsAttr = fd[0][1]
    for row in results:
        sentence = row[0]
        lhs = row[1]
        rhs = row[2]
        #print(sentence, lhs, rhs)
        queryData = fdTemplateQuery
        print("*** lhsAttr: ", lhsAttr)
        print("*** rhsAttr: ", rhsAttr)
        print("*** rhs: ", rhs)
        queryData = queryData.replace('$LHS$', ('"' + lhsAttr + '"')) #TODO: this line fails because it's a list
        queryData = queryData.replace('$RHS$', ('"' + rhsAttr + '"')) #TODO: this line fails because it's a list
        queryData = queryData.replace('$TABLE$', tableName)
        queryData = queryData.replace('$VALUE$', "'"+rhs+"'")
        queryData = queryData.replace('$PK$', ('"' + pk + '"'))
        dataTable = executeQueryBatch(queryData, connection)
        cQ = getColumnsName(queryData, connection)
        tupleColumn = tuple(cQ)
        dataTable.insert(0, tupleColumn)
        to_totto.append((sentence, dataTable))
    return to_totto







