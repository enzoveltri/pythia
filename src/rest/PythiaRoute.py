import csv
import os

from fastapi import APIRouter, Depends, UploadFile, Form

from src.pythia.Constants import STRATEGY_SCHEMA
from src.pythia.T5Engine import T5Engine
from src.rest.Authentication import User, get_current_active_user
from src.pythia.DBUtils import getScenarioListFromDb, getScenarioFromDb, deleteScenarioFromDb, existsDTModelInDb, \
    insertScenario, getEngine, updateScenario
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
    file_location = f"tmp/{file.filename}"
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
    #dataSet.storeInDB(file_location, user.username, separator)
    os.remove(file_location)
    return name


@pythiaroute.get("/dataframe/{name}")
def getDataframe(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
    ## TODO: return scenario.dataframe.head(10).to_html(classes='table table-striped table-bordered')
    return scenario.dataframe.to_html(classes='table table-striped table-bordered')


@pythiaroute.delete("/delete/{name}")
def deleteScenario(name: str, user: User = Depends(get_current_active_user)):
    deleteScenarioFromDb(name)


@pythiaroute.get("/get/{name}")
def getScenario(name: str, user: User = Depends(get_current_active_user)):
    scenario = getScenarioFromDb(name)
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







