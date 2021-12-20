from sqlalchemy import create_engine
from loguru import logger

def toSQLTable(dataFrame, tableName , username, password, address, port, dbName, lowerCaseColumnNames=True):
    connectionString = "postgresql://"+str(username) +":"+ str(password) + "@"+str(address) + ":" + str(port) +"/" + str(dbName)
    logger.debug("Connection String:", connectionString)
    #engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
    engine = create_engine(connectionString)
    if lowerCaseColumnNames:
        dfLowerCase = dataFrame.copy()
        dfLowerCase.columns = dfLowerCase.columns.str.lower()
        dfLowerCase.to_sql(tableName, engine)
    else:
        dataFrame.to_sql(tableName, engine)
    logger.debug("Data exported to ", tableName)


