import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """This module creates the database and returns the cursor and connection handle to be used for table creation and dropping the tables"""
    
    # connect to default database
    conn = psycopg2.connect("dbname=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("dbname=sparkifydb")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """This module drop tables based on cursor and connection handle received as parameters for the databse created"""
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This module creates tables based on cursor and connection handle received as parameters for the databse created"""
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This module calls appropriate modules to create database, drop tables if they already exist and the create the tables"""
    
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
