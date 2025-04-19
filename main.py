import psycopg2
from config import config

def connect():
    connection = None
    try:
        params = config()
        print('connting to postgreSQL...')
        connection = psycopg2.connect(**params)

        #create a cursor
        crsr = connection.cursor()
        print('PG database version: ')
        crsr.execute('SELECT version()')
        db_version = crsr.fetchone()
        print(db_version)
    except(Exception, psycopg2.DataError) as error:
        print(error)
    finally:
        if connection != None:
            connection.close()