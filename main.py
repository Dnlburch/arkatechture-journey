import psycopg2
from config import config

def connect():
    connection = None
    try:
        params = config()
        # print(params)
        print('connting to postgreSQL...')
        connection = psycopg2.connect(**params)

        # create a cursor
        crsr = connection.cursor()
        print('PG database version: ')
        crsr.execute('SELECT version()')
        db_version = crsr.fetchone()
        print(db_version)
        crsr.close()

    except(Exception, psycopg2.DataError) as error:
        print(error)

    finally:
        if connection != None:
            connection.close()
            print('DB connection closed')

if __name__ == '__main__':
    connect()