#Postgres
import psycopg2
import json
import sqlite3
import logging
try:
    import psycopg2
except ImportError:
    psycopg2 = None

def load_connection_details(connections_file):
    details={}
    try:
        with open(connections_file,'r') as file:
            connection_details=json.load(file)
            return connection_details
    except Exception as e:
        print(f"Error loading connection details: {e}")
        return None

def connect_local_sqlitedb(db_file):
    try:
        conn=sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(f"Error connecting to SQLite database: {e}")
        return None
    
def connect_to_postgresdb(details):
    try:
        conn=psycopg2.connect(
            host=details['host'],
            port=details['port'],
            user=details['user'],
            password=details['password'],
            dbname=details['dbname']
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None
    
def export_sqlite_to_json(sqlite_conn, output_file):
    try:
        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cursor.fetchall()]
        export_data = {"schema": {}, "data": {}}

        for table in tables:
            cursor.execute(f"PRAGMA table_info({table});")
            schema_info = [{"name": col[1], "type": col[2]} for col in cursor.fetchall()]
            export_data["schema"][table] = schema_info

            cursor.execute(f"SELECT * FROM {table};")
            rows = cursor.fetchall()
            export_data["data"][table] = rows

        with open(output_file, 'w') as file:
            json.dump(export_data, file, indent=2)

        return True
    except Exception as e:
        print(f"Error exporting SQLite database to JSON: {e}")
        return False


def import_json_to_target(target_conn, target_type, import_path):
    """
    Imports data from a JSON file into a target database.

    Parameters:
    - target_conn: The database connection object for the target database.
    - target_type: A string representing the type of the target database (e.g., "postgres", "mysql", "sqlite").
    - import_path: Path to the JSON file containing the data to be imported.

    The JSON file is expected to have a structure with tables as keys and a list of tuples (rows) as values.
    """

    # Load data from the JSON file
    with open(import_path, 'r', encoding='utf-8') as f:
        data_structure = json.load(f)

    cursor = target_conn.cursor()

    try:
        # Iterate through tables and data in the JSON file
        for table, rows in data_structure.get("data", {}).items():
            logging.info(f"Importing into table: {table}")

            if not rows:
                continue

            # Fetch column names from schema
            if table in data_structure.get("schema", {}):
                columns = [column["name"] for column in data_structure["schema"][table]]
            else:
                if target_type == "sqlite":
                    cursor.execute(f"PRAGMA table_info({table});")
                    columns = [info[1] for info in cursor.fetchall()]
                elif target_type == "postgres" or target_type == "mysql":
                    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' AND table_schema='public';")
                    columns = [info[0] for info in cursor.fetchall()]
                else:
                    raise ValueError(f"Unsupported database type: {target_type}")

            # Create table if it doesn't exist
            if target_type == "postgres":
                # Create table query based on the schema
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table} ("
                column_defs = [f"{col} TEXT" for col in columns]  # Default to TEXT for all columns (can be improved for types)
                create_table_sql += ", ".join(column_defs) + ");"
                cursor.execute(create_table_sql)
                logging.info(f"Table created: {table}")

            # Construct column and placeholder strings
            col_string = ",".join(columns)
            placeholders = ",".join(["%s"] * len(columns)) if target_type in ["postgres", "mysql"] else ",".join(["?"] * len(columns))

            # Prepare the SQL query
            sql = f"INSERT INTO {table} ({col_string}) VALUES ({placeholders})"

            # Insert rows into the table
            for row in rows:
                if len(row) != len(columns):
                    logging.warning(f"Skipping row with incorrect number of columns: {row}")
                    continue
                cursor.execute(sql, row)

        # Commit the changes
        logging.info("Committing changes to the database.")
        target_conn.commit()
        logging.info("Data import completed successfully.")

    except Exception as e:
        target_conn.rollback()
        logging.error(f"Error importing data into table: {e}")
        raise
    finally:
        cursor.close()
        logging.info("Database connection closed.")

def check_data_transfer(target_conn, table_name):
    
    try:
        cursor = target_conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]

        logging.info(f"Data transfer check for table '{table_name}': {row_count} rows found.")

        return row_count

    except Exception as e:
        logging.error(f"Error checking data in table '{table_name}': {e}")

    finally:
        cursor.close()
