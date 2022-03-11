import json

import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from munch import DefaultMunch

## SCENARIO
from src.pythia.Dataset import Dataset
from src.pythia.Metadata import Metadata
from src.pythia.User import User

## DB PARAMETER
# TODO: read from config file
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "pythia"


def getScenarioListFromDb(username):
    query = "select * from Scenari where username='" + username + "'; "
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    return executeQueryBatch(query, connection)


def getScenarioFromDb(name):
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    query = "select * from scenari where name='" + name + "';"
    print("*** query: ", query)
    meta = executeQueryBatch(query, connection)
    #dict = json.loads(meta[0][2])
    dict = meta[0][2]
    #dataset = Dataset(**json.loads(meta[0][2]))
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


def getTemplatesFromDb(name):
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    query = "select * from scenari where name='" + name + "';"
    meta = executeQueryBatch(query, connection)
    templates = []
    result = json.loads(meta[0][3])
    for t in result:
        templates.append((t['query'], t['name'], t['config']))
    return templates


def getAmbiguousFromDb(name):
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    query = "select * from " + name + "_ambiguous;"
    ambiguous = executeQueryBatch(query, connection)
    results = []
    for a in ambiguous:
        results.append(((a[0], a[1]), (a[2], a[3]), a[4]))
    return results


def existsDTModelInDb(name):
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    query1 = "select * from scenari where name='" + name + "';"
    result = executeQueryBatch(query1, connection)
    return len(result) > 0


def insertScenario(name, username, dataset):
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
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


def updateScenario(name, dataset):
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    query = "UPDATE scenari set metadata='" + dataset.toJSON() + "' WHERE name='" + name + "';"
    print(query)
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
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
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
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
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
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    query = "drop table " + name + "; drop table " + name + "_ambiguous; delete from scenari where name='" + name + "';"
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
    connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
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
#         connection = getDBConnection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
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