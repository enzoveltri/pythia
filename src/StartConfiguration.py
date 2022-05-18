import sys
from pathlib import Path

import psycopg2
import configparser, os
from passlib.context import CryptContext


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# service methods
def initPythiaDb(configFilePath):
    config = readConfigParameters(configFilePath)
    connection = getDBConnection(config['db']['user'], config['db']['password'], config['db']['host'], config['db']['port'], config['db']['dbname'])
    hash_password = pwd_context.hash('admin')
    query = "CREATE TABLE scenari (" \
            "name character varying NOT NULL," \
            "username character varying NOT NULL," \
            "metadata json," \
            "templates json," \
            "deltas json" \
            ");"

    query += "CREATE TABLE users (" \
             "username character varying NOT NULL," \
             "full_name character varying," \
             "password character varying," \
             "email character varying" \
             ");"

    query += "CREATE TABLE ambiguous_cache (" \
             "attr1 character varying," \
             "attr2 character varying," \
             "label character varying" \
             ");"

    ## passw admin
    query += "INSERT INTO users (username, full_name, password, email)"\
             " VALUES ('admin','admin','" + hash_password + "', 'admin@email.it');"

    try:
        cur = connection.cursor()
        cur.execute(query)
        cur.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: ", error)
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()


def readConfigParameters(configFilePath):
    config = configparser.ConfigParser()
    config.read(configFilePath)
    return config


def getDBConnection(user_uenc, pw_uenc, host, port, dbname):
    connection = psycopg2.connect(user = user_uenc, password = pw_uenc, host = host, port = port, database = dbname)
    return connection


def testDBConnection(user_uenc, pw_uenc, host, port, dbname):
    try:
        conn = psycopg2.connect(user = user_uenc, password = pw_uenc, host = host, port = port, database = dbname, connect_timeout=1)
        conn.close()
        return True
    except:
        return False

# START CONFIGURATION
print("*** Welcome to Pythia configuration ***")
print("-------------------------------------------------------------------------------------------------")
print("Pythia requires PostgreSQL. Make sure you have already created an empty database before starting.")
print("-------------------------------------------------------------------------------------------------")
print("-- Insert DB parameters: ---")
user = str(input("Enter username: "))
password = str(input("Enter password: "))
host = str(input("Enter host: "))
port = int(input("Enter port: "))
dbname = str(input("Enter DB name: "))

if not testDBConnection(user, password, host, port, dbname):
    print("Error DB connection")
    sys.exit()

config = configparser.ConfigParser()
configFileName = "config.ini"
directory = Path(os.path.dirname(os.path.realpath(__file__)))
path = os.path.join(directory.parent.absolute(), configFileName)
print("*** path: ", path)
#if not os.path.exists(path):
config['db'] = {'user': user, 'password': password, 'host': host, 'port': port, 'dbname': dbname}
config['params'] = {'cache': True, 'maxaqueries': 5, 'shuffle': True}
config['t5'] = {'localpath': '../../data/model', 'remoteurl': '', 'user': '', 'password': ''}
config.write(open(path, 'w'))

print("*** Creating tables... ")
initPythiaDb(path)
print("*** End of configuration ***")



