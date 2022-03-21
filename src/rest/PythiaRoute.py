import csv
import json
import os

from fastapi import APIRouter, Depends, UploadFile, Form

from src.pythia.Constants import STRATEGY_SCHEMA
from src.pythia.Pythia import find_a_queries
from src.pythia.T5Engine import T5Engine
from src.pythia.TemplateFactory import TemplateFactory, getTemplatesByType
from src.rest.Authentication import User, get_current_active_user
from src.pythia.DBUtils import getScenarioListFromDb, getScenarioFromDb, deleteScenarioFromDb, existsDTModelInDb, \
    insertScenario, getEngine, updateScenario, updateTemplates, getTemplatesFromDb, getDBConnection
from src.pythia.Dataset import Dataset

t5Engine = T5Engine().getInstance()

## DB PARAMETER
# TODO: read from config file
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "pythia"

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
    file_location = f"../../data/tmp/{file.filename}"
    name = file.filename.replace(".csv", "")
    datasetName = user.username + "_" + name
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    dataset = Dataset(datasetName)
    dataset.initDataframe(file_location)
    attributes = dataset.getAttributes()
    df = dataset.getDataFrame()
    dataset.findPK()
    engine = getEngine(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    checkDB = dataset.storeInDB(engine)
    print("*** Check DB: ", checkDB)
    insertScenario(datasetName, user.username, dataset)
    templateFactory = TemplateFactory()
    templates = templateFactory.templatesToJson()
    updateTemplates(datasetName, templates)
    os.remove(file_location)
    return datasetName


@pythiaroute.get("/dataframe/{name}")
def getDataframe(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    ## TODO: manage pagination
    return scenario.dataframe.iloc[0:10].to_html(classes='table table-striped table-bordered table-sm')


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

@pythiaroute.get("/find/pk/{name}")
def findPrimaryKey(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    scenario.findPK()
    updateScenario(name, scenario)
    scenario = getScenarioFromDb(name) ## TODO: remove issue with delattr in toJSON
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
    scenario = getScenarioFromDb(name) ## TODO: remove issue with delattr in toJSON
    return scenario.toJSON()

@pythiaroute.get("/find/fds/{name}")
def findFDs(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    scenario.findFDs(scenario.dataframe, True)
    fds = scenario.getFDs()
    rowMeaning = ["has", "in"] ## TODO: parameter for the next function
    scenario.extendFDs(rowMeaning)
    updateScenario(name, scenario)
    scenario = getScenarioFromDb(name)  ## TODO: remove issue with delattr in toJSON
    return scenario.toJSON()

@pythiaroute.get("/find/ambiguous/{name}")
def findAmbiguous(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    strategy = STRATEGY_SCHEMA ## TODO: Parameter
    scenario.findAmbiguousAttributes(strategy, t5Engine)
    ambiguousAttributes = scenario.getAmbiguousAttribute()
    print(len(ambiguousAttributes))
    updateScenario(name, scenario)
    scenario = getScenarioFromDb(name)  ## TODO: remove issue with delattr in toJSON
    return scenario.toJSON()


@pythiaroute.post("/save/{name}")
def saveScenario(name: str,  scenario = Form(...), user: User = Depends(get_current_active_user)):
    updateScenario(name, scenario, False)
    scenario = getScenarioFromDb(name)  ## TODO: remove issue with delattr in toJSON
    return scenario.toJSON()


@pythiaroute.post("/predict/{name}")
def predict(name: str, strategy: str = Form(...), structure: str = Form(...), user: User = Depends(get_current_active_user)):
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    scenario = getScenarioFromDb(name)
    totalTemplates = getTemplatesFromDb(name)
    print("Schema: ", STRATEGY_SCHEMA)
    print("Strategy: ", strategy)
    print("Structure: ", structure)
    templates = getTemplatesByType(totalTemplates, structure)
    a_queries, a_queries_with_data = find_a_queries(scenario, templates, strategy, connection,
                                                    operators=["=", ">", "<"], functions=["min", "max"],
                                                    executeQuery=True, limitQueryResults=10, shuffleQuery=True)
    print("Total A-Queries Generated:", len(a_queries))
    print("Differents A-Queries: ", len(set(a_queries)))
    result = []
    if len(set(a_queries)) > 0:
        for r in a_queries_with_data:
            result.append(r)
    return result










