import psycopg2
import logging

# file name for logs: filename='./MyFirstETLPipeline/logs/app.log'
def init_logger():
    logging.basicConfig( level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Start of Program And Logger initialized.")

# Test PostgreSQL connection
def psql_connection_test():
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(host="localhost", database="MyFirstETLPipelineDB", user ="postgres", password="postgres")

        #create a cursor
        cur = conn.cursor()

        #execute a statement
        logging.info('PostgreSQL database version:')
        cur.execute('SELECT version()')

        #display the PostgreSQL databse server version
        db_version = cur.fetchone()
        logging.info(db_version)

        #close the communication with the PostgreSQL
        cur.close()

    except(Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')
            
# creating the connection
def psql_connection():
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # connect to the PostgreSQL server
        logging.info("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(host="localhost", database="MyFirstETLPipelineDB", user ="postgres", password="postgres")
        return conn
    except(Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def create_table(psql_conn):
    pass

def insert_member(psql_conn, member):
    pass

