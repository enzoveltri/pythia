import csv
import json
import os
import random

from fastapi import APIRouter, Depends, UploadFile, Form

from src.pythia.Constants import *
from src.pythia.DeltaAmbiguous import DeltaAmbiguous
from src.pythia.ExportResult import ExportResult
from src.pythia.Pythia import find_a_queries, checkWithData
from src.pythia.StringUtils import normalizeString
from src.pythia.T5Engine import T5Engine
from src.pythia.TemplateFactory import TemplateFactory, getTemplatesByName, getOperatorsFromTemplate
from src.rest.Authentication import User, get_current_active_user
from src.pythia.DBUtils import getScenarioListFromDb, getScenarioFromDb, deleteScenarioFromDb, existsDTModelInDb, \
    insertScenario, getEngine, updateScenario, updateTemplates, getTemplatesFromDb, getDBConnection, getColumnsName, \
    getDbUser, getDbPassword, getDbHost, getDbPort, getDbName, executeQueryBatch, readConfigParameters, getDeltasFromDb, \
    updateDeltas, getAmbiguousCacheFromDb, updateAmbiguousInCache, insertAmbiguousInCache
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
    name = file.filename.replace(".csv", "").lower()
    datasetName = user.username + "_" + normalizeString(name,"_")
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    dataset = Dataset(datasetName, name)
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
    oldScenario = getScenarioFromDb(name)
    updateScenario(name, scenario, False)
    scenario = getScenarioFromDb(name)
    # Update cache
    config = readConfigParameters()
    cacheEnabled = config.getboolean('params', 'cache')
    if cacheEnabled:
        ambiguous = dict()
        for ambOld in oldScenario.getAmbiguousAttribute():
            requestString = oldScenario._linearizeSchema() + " attr1: " + ambOld[0].name + " attr2: " + ambOld[1].name
            ambiguous[requestString] = "none"
        for amb in scenario.getAmbiguousAttribute():
            requestString = scenario._linearizeSchema() + " attr1: " + amb[0].name + " attr2: " + amb[1].name
            ambiguous[requestString] = amb[2]
        for key in ambiguous:
            if getAmbiguousCacheFromDb(key):
                updateAmbiguousInCache(key, ambiguous[key])
            else:
                insertAmbiguousInCache(key, ambiguous[key])
    # Update deltas
    deltas = getDeltasFromDb(name)
    for d in get_deltas_ambiguous(oldScenario, scenario):
        deltas.append(d)
    updateDeltas(name, json.dumps([d.toJSON() for d in deltas], indent=3))
    return scenario.toJSON()

@pythiaroute.post("/save/templates/{name}")
def saveTemplates(name: str,  templates = Form(...), user: User = Depends(get_current_active_user)):
    temps = json.loads(templates)
    for t in temps:
        if t[2]:
            ops = []
            for op in t[2]:
                if "," in op:
                    op = op.split(",")
                ops.append(op)
            t[2] = ops
    templates = json.dumps(temps)
    templates = templates.replace("''", "'").replace("'", "''")
    updateTemplates(name, templates)
    templates = getTemplatesFromDb(name)
    return json.dumps(templates)

@pythiaroute.get("/maxaqueries")
def getMaxAQueries(user: User = Depends(get_current_active_user)):
    config = readConfigParameters()
    return config['params']['maxaqueries']

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
    config = readConfigParameters()
    shuffle = config.getboolean('params', 'shuffle')
    a_queries, a_queries_with_data = find_a_queries(scenario, templates, strategy, connection,
                                                    operators, functions=["min", "max"],
                                                    executeQuery=True, limitQueryResults=limitResults, shuffleQuery=shuffle)
    print("*** Total A-Queries Generated:", len(a_queries))
    print("*** Differents A-Queries: ", len(set(a_queries)))

    result = get_results(connection, strategy, scenario, a_queries_with_data)
    tName = scenario.datasetName
    return result


@pythiaroute.post("/predict/single/{name}/{index}")
def predictSingleAQuery(name: str, index: int, aQuery: str = Form(...), strategy: str = Form(...), structure: str = Form(...), limitResults: int = Form(...), user: User = Depends(get_current_active_user)):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    scenario = getScenarioFromDb(name)
    totalTemplates = getTemplatesFromDb(name)
    template = getTemplatesByName(totalTemplates, structure)[0]
    type = template[1]
    fd = None #TODO: manage fds
    a_queries_with_data = []
    stored_results = []

    print("*** name: ", name)
    print("*** index: ", index)
    print("*** aQuery: ", aQuery)
    print("*** strategy: ", strategy)
    print("*** structure: ", structure)
    print("*** limitResults: ", limitResults)
    print("*** type: ", type)

    checkWithData(aQuery, type, connection, a_queries_with_data, stored_results, template, fd, 0) #TODO: manage limit results inside checkWithData function

    print("*** a_queries_with_data: ", a_queries_with_data)
    print("*** stored_results: ", stored_results)

    result = get_results(connection, strategy, scenario, stored_results)
    tName = scenario.datasetName
    return result[0]


def get_results(connection, strategy, scenario, a_queries_with_data):
    result = []
    index = 0
    for a_query, type, results, template, fd in a_queries_with_data:
        tables_from_sentences = []
        dict_from_sentences = []
        to_totto = []
        if (type == TYPE_FD):
            tableName = scenario.datasetName
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_fd(results, columnsQuery, tableName, connection)
            tables_from_sentences = get_tables_from_sentences(to_totto)
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
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_full(results, columnsQuery)
            tables_from_sentences = get_tables_from_sentences(to_totto)
            dict_from_sentences = get_dict_from_sentences(to_totto)
        if (type == TYPE_FUNC):
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_full(results, columnsQuery)
            tables_from_sentences = get_tables_from_sentences(to_totto)
            dict_from_sentences = get_dict_from_sentences(to_totto)
            #print(results)
        export_results = []
        for i in range(len(results)):
            if len(dict_from_sentences) > 0:  # TODO: remove if clause when all the to_totto are ok
                export_results.append(
                    ExportResult(results[i][0], a_query, template[1], strategy, dict_from_sentences[i]))
            # export_results.append(ExportResult(results[i][0], a_query, template[1], strategy, dict_from_sentences[i]))
        result.append((index, a_query, type, results, template, fd, tables_from_sentences, export_results))
        index += 1
    return result


#TODO: Move next methods to appropriate class

def get_deltas_ambiguous(oldScenario, newScenario):
    listOld = oldScenario.getAmbiguousAttribute()
    listNew = newScenario.getAmbiguousAttribute()
    result = []
    for n in listNew:
        result.append(DeltaAmbiguous(newScenario._linearizeSchema(), n[0].name, n[1].name, None, n[2]))
    for o in listOld:
        deltas = [d for d in result if d.attr1 == o[0].name and d.attr2 == o[1].name]
        if len(deltas) > 0:
            delta = deltas[0]
            if o[2] == delta.real:
                result.remove(delta)
            else:
                delta.predicted = o[2]
        else:
            result.append(DeltaAmbiguous(newScenario._linearizeSchema(), o[0].name, o[1].name, o[2], None))
    #print("results: ", json.dumps([d.toJSON() for d in result]))
    return result



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
        rowsToGenerate = 0
        lastColumn = ""
        for i in range(1, len(cols)):
            if cols[i] not in colList:
                rowsToGenerate = 1
                colList.append(cols[i])
                lastColumn = cols[i]
            if (cols[i] == lastColumn):
                rowsToGenerate += 1
        rowsToGenerate -= 1
        rows = []
        for i in range(0, rowsToGenerate):
            rowNew = []
            rows.append(rowNew)
        for i in range(1, len(cols), rowsToGenerate):
            #colList.append(cols[i])
            for col in range(0, rowsToGenerate):
                rowNew = rows[col]
                rowNew.append(row[i+col])
        data = [tuple(colList)]
        for rowNew in rows:
            data.append(tuple(rowNew))
        to_totto.append((sentence, data))
    return to_totto


def to_totto_row(results, cols):
    to_totto = []
    for row in results:
        sentence = row[0]
        colList = []
        rowsToGenerate = 0
        lastColumn = ""
        for i in range(1, len(cols)):
            if cols[i] not in colList:
                rowsToGenerate = 1
                colList.append(cols[i])
                lastColumn = cols[i]
            if (cols[i] == lastColumn):
                rowsToGenerate += 1
        rowsToGenerate -= 1
        rows = []
        for i in range(0, rowsToGenerate):
            rowNew = []
            rows.append(rowNew)
        for i in range(1, len(cols), rowsToGenerate):
            #colList.append(cols[i])
            for col in range(0, rowsToGenerate):
                rowNew = rows[col]
                rowNew.append(row[i+col])
        data = [tuple(colList)]
        for rowNew in rows:
            data.append(tuple(rowNew))
        to_totto.append((sentence, data))
    return to_totto

def to_totto_fd(results, columnsQuery, tName, connection):
    to_totto = []
    for row in results:
        sentence = row[0]
        fdAttrs = columnsQuery[1:]
        rhsAttr = columnsQuery[-1]
        rhsValue = row[-1]
        queryFD = "SELECT " + ", ".join(fdAttrs) + ", count(*) FROM " + tName + " WHERE " + rhsAttr + " = '" + rhsValue + "' GROUP BY " +  ", ".join(fdAttrs)
        fdResult = executeQueryBatch(queryFD, connection)
        dataTable = []
        tHeader = fdAttrs + ["count"]
        tColumns = tuple(tHeader)
        dataTable.append(tColumns)
        for rdf in fdResult:
            t = tuple(rdf)
            dataTable.append(t)
        to_totto.append((sentence, dataTable))
    return to_totto







