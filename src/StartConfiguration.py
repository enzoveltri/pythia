import psycopg2
import configparser, os


# service methods
def initPythiaDb():
    config = readConfigParameters()
    connection = getDBConnection(config['db']['user'], config['db']['password'], config['db']['host'], config['db']['port'], config['db']['dbname'])
    query = "CREATE TABLE scenari (" \
            "name character varying NOT NULL," \
            "username character varying NOT NULL," \
            "metadata json," \
            "templates json" \
            ");"

    query += "CREATE TABLE users (" \
             "username character varying NOT NULL," \
             "full_name character varying," \
             "password character varying," \
             "email character varying" \
             ");"

    query += "INSERT INTO users (username, full_name, password, email)"\
             " VALUES ('admin','admin','$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW', 'admin@email.it');"

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


def readConfigParameters():
    config = configparser.ConfigParser()
    configFilePath = "./config.ini"
    config.read(configFilePath)
    return config


def getDBConnection(user_uenc, pw_uenc, host, port, dbname):
    connection = psycopg2.connect(user = user_uenc, password = pw_uenc, host = host, port = port, database = dbname)
    return connection


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


config = configparser.ConfigParser()
configFilePath = "./config.ini"
if not os.path.exists(configFilePath):
    config['db'] = {'user': user, 'password': password, 'host': host, 'port': port, 'dbname': dbname}
    config.write(open(configFilePath, 'w'))

print("*** Creating tables... ")
initPythiaDb()
print("*** End of configuration ***")



