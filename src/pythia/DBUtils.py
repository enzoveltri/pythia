from sqlalchemy import create_engine
import psycopg2

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
#         connection = getDBConnection()
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