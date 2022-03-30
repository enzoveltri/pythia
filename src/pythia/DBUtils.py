import configparser
import json
import os

import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from munch import DefaultMunch

## SCENARIO
from src.pythia.Dataset import Dataset
from src.pythia.Metadata import Metadata
from src.pythia.User import User


# DB PARAMETER
def readConfigParameters():
    config = configparser.ConfigParser()
    configFilePath = "../../config.ini"
    if not os.path.exists(configFilePath):
        print("File non trovato!")
    config.read(configFilePath)
    return config


def getDbUser():
    config = readConfigParameters()
    return config['db']['user']


def getDbPassword():
    config = readConfigParameters()
    return config['db']['password']


def getDbHost():
    config = readConfigParameters()
    return config['db']['host']


def getDbPort():
    config = readConfigParameters()
    return config['db']['port']


def getDbName():
    config = readConfigParameters()
    return config['db']['dbname']


def getScenarioListFromDb(username):
    query = "select * from Scenari where username='" + username + "'; "
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    return executeQueryBatch(query, connection)


def getScenarioFromDb(name):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    query = "select * from scenari where name='" + name + "';"
    meta = executeQueryBatch(query, connection)
    # dict = json.loads(meta[0][2])
    dict = meta[0][2]
    # dataset = Dataset(**json.loads(meta[0][2]))
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
    dataset.dataframe = pd.read_sql(query, connection)
    return dataset

def getHeadersToQuery(dataset):
    attrs = []
    for att in dataset.attributes:
        attrs.append(att.normalizedName + ' as "' + att.name + ' (' + att.type[0:3] + ')"')
    return "select " + ','.join(attrs) + " from " + dataset.datasetName


def getTemplatesFromDb(name):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    query = "select * from scenari where name='" + name + "';"
    meta = executeQueryBatch(query, connection)
    return meta[0][3]


def getAmbiguousFromDb(name):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    query = "select * from " + name + "_ambiguous;"
    ambiguous = executeQueryBatch(query, connection)
    results = []
    for a in ambiguous:
        results.append(((a[0], a[1]), (a[2], a[3]), a[4]))
    return results


def existsDTModelInDb(name):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    query1 = "select * from scenari where name='" + name + "';"
    result = executeQueryBatch(query1, connection)
    return len(result) > 0


def insertScenario(name, username, dataset):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    query = "INSERT INTO scenari(name, username, metadata) VALUES ('" + name + "','" + username + "','" + dataset.toJSON() + "'); "
    try:
        cur = connection.cursor()
        cur.execute(query)
        cur.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Errore: ", error)
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()


def updateScenario(name, dataset, convertToJson = True):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    if convertToJson:
        dataset = dataset.toJSON()
    query = "UPDATE scenari set metadata='" + dataset + "' WHERE name='" + name + "';"
    try:
        cur = connection.cursor()
        cur.execute(query)
        cur.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Errore: ", error)
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()

def createAmbiguousTable(name):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    query = "CREATE TABLE " + name + "_ambiguous (" \
                                     "attr1 VARCHAR ( 50 ) NOT NULL," \
                                     "type_attr1 VARCHAR ( 50 ) NOT NULL," \
                                     "attr2 VARCHAR ( 50 ) NOT NULL, " \
                                     "type_attr2 VARCHAR ( 50 ) NOT NULL," \
                                     "label VARCHAR ( 50 ) NOT NULL " \
                                     ");"
    try:
        cur = connection.cursor()
        cur.execute(query)
        cur.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Errore: ", error)
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()


def updateTemplates(name, templates):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    query = "UPDATE scenari set templates='" + templates + "' where name='" + name + "'; "
    try:
        cur = connection.cursor()
        cur.execute(query)
        cur.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Errore: ", error)
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()



def deleteScenarioFromDb(name):
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    query = "drop table " + name + "; "
    #query += "drop table " + name + "_ambiguous; " TODO: to enable when this table creation is enabled
    query += "delete from scenari where name='" + name + "'; "
    try:
        cur = connection.cursor()
        cur.execute(query)
        cur.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Errore: ", error)
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()


## AUTH
def getUserFromDb(username):
    query = "select * from users where username='" + username + "';"
    connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
    results = executeQueryBatch(query, connection)
    if len(results) > 0:
        user = User
        user.username = results[0][0]
        user.password = results[0][2]
        user.email = results[0][3]
        user.full_name = results[0][1]
        user.disabled = None
        return user
    else:
        return None


## UTILS
def getDBConnection(user_uenc, pw_uenc, host, port, dbname) :
    connection = psycopg2.connect(user = user_uenc, password = pw_uenc, host = host, port = port, database = dbname)
    return connection

def getEngine(username, password, address, port, dbName):
    connectionString = "postgresql://"+str(username) +":"+ str(password) + "@"+str(address) + ":" + str(port) +"/" + str(dbName)
    engine = create_engine(connectionString)
    return engine

# def executeQuery (query):
#     results = [];
#     try:
#         connection = getDBConnection(getDbUser(), getDbPassword(), getDbHost(), getDbPort(), getDbName())
#         cursor = connection.cursor()
#         cursor.execute(query)
#         results = cursor.fetchall()
#     except (Exception, psycopg2.Error) as error :
#         print ("Error while connecting to PostgreSQL", error)
#     finally:
#             if(connection):
#                 cursor.close()
#                 connection.close()
#     return results

def executeQueryBatch (query, connection):
    results = []
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
    except (Exception, psycopg2.Error) as error :
        #print(query)
        #print ("Error while connecting to PostgreSQL", error)
        connection.rollback()
    return results

def getColumnsName(query, connection):
    columnNamesQ = []
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        columnNamesQ = [desc[0] for desc in cursor.description]
    except (Exception, psycopg2.Error) as error :
        print(query)
        print ("Error while connecting to PostgreSQL", error)
    return columnNamesQ