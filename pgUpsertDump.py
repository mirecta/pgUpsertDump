#!/usr/bin/python3
import argparse
import logging
import psycopg2
import psycopg2.extensions

import sys
import getpass

#Binary("\x00\x08\x0F").getquoted()

def escape_value(value):
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        #print(psycopg2.extensions.QuotedString(value.encode('utf-8')).getquoted())
        #print(psycopg2.extensions.QuotedString(value.encode('utf-8')).getquoted().decode('utf-8'))
        return psycopg2.extensions.QuotedString(value.encode('utf-8')).getquoted().decode('utf-8')
    else:
        return str(value)

def getPK(cursor, table_name):
    cursor.execute(f"""SELECT column_name
                        FROM information_schema.table_constraints
                        JOIN information_schema.key_column_usage
                            USING (constraint_catalog, constraint_schema, constraint_name,
                                table_catalog, table_schema, table_name)
                        WHERE constraint_type = 'PRIMARY KEY'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position""")
    pks = cursor.fetchall()
    keys = [pk[0] for pk in pks]
    return keys

def list_tables(conn, tables=None):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()
        
        # Get all table names in the given database
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        
        # Fetch all the results
        all_tables = cursor.fetchall()
        
        # Filter the tables based on the provided table names
        if tables:
            filtered_tables = [table for table in all_tables if table[0] in tables]
        else:
            filtered_tables = all_tables
        
        # Print the table names and their columns
        logging.info("Tables in the database:")
        for table in filtered_tables:
            table_name = table[0]
            logging.info("Table: %s", table_name)
            
            # Get all column names for the current table
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            
            # Fetch all the column names
            columns = cursor.fetchall()
            
            # Print the column names
            column_names = [column[0] for column in columns]
            logging.info("Columns: %s", ", ".join(column_names))
            
            # Get table data
            cursor.execute(f"SELECT {', '.join(column_names)} FROM {table_name}")
            
            # Fetch all the rows
            rows = cursor.fetchall()
            
            #PK
            keys = getPK(cursor, table_name)

            if len(rows) == 0:
                logging.info("No data in the table")
            else:
                # Generate INSERT statements with ON CONFLICT DO UPDATE
                logging.info(f"Dumping data for table {table_name}")
                for row in rows:
                    row_values = [escape_value(value) for value in row]
                    values_str = ", ".join(row_values)
                    insert_stmt = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({values_str}) ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in column_names])};"
                    print(insert_stmt)
            
        
        # Close the cursor (no need to close the connection when passed as an argument)
        cursor.close()
    
    except psycopg2.Error as e:
        logging.error("Error connecting to the PostgreSQL database: %s", e)


def main():
    # Create an argument parser
    parser = argparse.ArgumentParser(description="List tables, columns, and data in a PostgreSQL database")
    
    # Add command-line arguments
    parser.add_argument('-d', '--database', required=True, help="the database name")
    parser.add_argument('-H', '--host', default='/var/run/postgresql', help="the host address")
    parser.add_argument('-p', '--port', default='5432', help="the port number")
    parser.add_argument('-U', '--username', default=getpass.getuser(), help="the username")
    parser.add_argument('-W', '--password', default = '', help="the password")
    parser.add_argument('-t', '--tables', nargs='+', help="specific table names to list columns and data")
    #parser.add_argument('-h', '--help', action='help', help="show this help message and exit")
    # Parse the command-line arguments
    args = parser.parse_args()
    
    # Configure logging to stderr
    logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stderr)
    
    try:
        conn = psycopg2.connect(
            database=args.database,
            host=args.host,
            port=args.port,
            user=args.username,
            password=args.password
        )

                # Call the function with the provided arguments
        list_tables(
            conn=conn,
            tables=args.tables,
        )

        # Close the connection
        conn.close()

    except psycopg2.Error as e:
        logging.error("Error connecting to the PostgreSQL database: %s", e)
        sys.exit(1)


if __name__ == '__main__':
    main()

