from utilities.helper_psql import init_logger, psql_connection_test, psql_connection, create_table

from utilities.get_data import get_selfdev_members

if __name__ == "__main__":
    init_logger()
    
    # psql_connection_test()  # Testing connection
    conn = psql_connection() # getting the connection object
    create_table(conn) # creating the table if not exists
    get_selfdev_members(conn) # getting and inserting the data