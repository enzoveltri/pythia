import csv
import os

from fastapi import APIRouter, Depends, UploadFile, Form

from src.pythia.Authentication import User, get_current_active_user
from src.pythia.DBUtils import getScenarioListFromDb
from src.pythia.Dataset import Dataset

pythiaroute = APIRouter()

@pythiaroute.get("/get")
def getScenarios(user: User = Depends(get_current_active_user)):
    list = getScenarioListFromDb(user.username)
    result = {}
    for row in list:
        result[row[0]] = row[0].replace(user.username + "_", "")
    return result


@pythiaroute.post("/uploadFile")
def uploadFile(file: UploadFile = Form(...), user: User = Depends(get_current_active_user)):
    file_location = f"tmp/{file.filename}"
    name = file.filename.replace(".csv", "")
    datasetName = user.username + "_" + name
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    with open(file_location, "r") as file_read:
        dialect = csv.Sniffer().sniff(file_read.readline(), delimiters=";,")
        separator = dialect.delimiter
    dataSet = Dataset(datasetName)
    dataSet.storeInDB(file_location, separator)
    os.remove(file_location)
    return dataSet.df