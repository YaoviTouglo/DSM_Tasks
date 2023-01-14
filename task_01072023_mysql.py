import mysql.connector as mysql
from mysql.connector import errorcode
import pandas as pd


def import_csv_data(file):
    df_to_list = []
    try:
        df = pd.read_csv(file, sep=',', index_col=None)
        df_to_list = df.values.tolist()
    except:
        print("import_csv_data() --> ERROR: Something went wrong with reading the data file.")

    return df_to_list


# Define a connection function for MySQL database
def connect_db(cfg):
    conn = None
    try:
        conn = mysql.Connect(**cfg)
        cursor = conn.cursor(prepared=True)
    except mysql.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("ERROR: Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        conn = None
    return conn


def check_current_db(conn):
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchall()
    print(record)


def create_db(conn, dbname):
    try:
        cursor = conn.cursor()
        stmt = 'CREATE DATABASE IF NOT EXISTS {0};'.format(dbname)
        cursor.execute(stmt)
        cursor.close()
    except:
        print("create_db() --> ERROR: Something went wrong with creating the database {}.".format(dbname))


def create_table(conn, dbname, tblname):
    try:
        cursor = conn.cursor()
        stmt_create_tbl = """CREATE TABLE if not exists  {0}.{1}(
            id int NOT NULL, ri float, na float, mg float,
            al float, si float, k float, ca float, ba float, fe float,
            class int );""".format(dbname, tblname)

        result = cursor.execute(stmt_create_tbl)
        cursor.close()
    except:
        print("create_table() --> ERROR: Something went wrong with creating the table {}.{}".format(dbname, tblname))


def bulk_insert_table(conn, dbname, tblname, data):
    try:
        cursor = conn.cursor()

        delete_stmt = "TRUNCATE TABLE {}.{}".format(dbname, tblname)
        cursor.execute(delete_stmt)

        insert_stmt = """INSERT INTO {}.{} (id, ri, na, mg, al, si, k, ca, ba, fe, class) 
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """.format(dbname, tblname)
        cursor.executemany(insert_stmt, data)
        conn.commit()

        print(cursor.rowcount, "Record inserted successfully into {}.{} table".format(dbname, tblname))
    except:
        print("bulk_insert_table() --> ERROR: Something went wrong with inserting the table {}.{}.".format(dbname,
                                                                                                           tblname))


def main(dbconfig, dbname, tblname, datafile):
    conn = connect_db(dbconfig)

    if conn.is_connected():
        create_db(conn, dbname)
        create_table(conn, dbname, tblname)
        df_to_list = import_csv_data(datafile)
        bulk_insert_table(conn, dbname, tblname, df_to_list)

        conn.close()
    else:
        print("main() --> ERROR: The connection to the database {} is not established".format(dbname))


if __name__ == '__main__':
    dbconfig = {
        'host': 'localhost',
        'port': 3310,
        'user': 'abc',
        'password': 'password'
    }
    db_name = "datascience"
    tbl_name = "glass"
    data_file = "glass.data"

    # Call main()
    main(dbconfig, db_name, tbl_name, data_file)
