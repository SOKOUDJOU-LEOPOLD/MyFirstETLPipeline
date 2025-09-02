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

# create table
def create_table(psql_conn):
    """ create tables in the PostgreSQL database"""
    q_create_table = """
        CREATE TABLE IF NOT EXISTS members (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            discord_id VARCHAR(255) NOT NULL,
            coding_motivation VARCHAR(255) NOT NULL,
            addiction VARCHAR(255) NOT NULL
        )
        """
    
    try:
        cur = psql_conn.cursor()
        # execute each query
        cur.execute(q_create_table)
        logging.info(f"Executed query {q_create_table}")
        #close communication with PostgreSQL
        cur.close()
        #Commit the changes
        psql_conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def insert_member(psql_conn, member):
    q_insert_member = """
        INSERT INTO members(name, discord_id, coding_motivation, addiction)
        VALUES(%s, %s, %s, %s)
        """
    try:
        cur = psql_conn.cursor()
        cur.execute(q_insert_member, (member['name'], member['discord_id'], member['coding_motivation'], member['addiction']))
        logging.info(f"Processing Member: {member['name']}")
        # commit changes
        psql_conn.commit()
    
    except(Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error in <insert_member> function {error}")
    finally:
        #close the communication
        cur.close()


