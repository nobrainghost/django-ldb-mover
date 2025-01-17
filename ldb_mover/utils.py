#Postgres
import psycopg2
import json
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
        for table, rows in data_structure.items():
            logging.info(f"Importing into table: {table}")

            if not rows:
                continue

            # Fetch column names by querying the target database schema
            cursor.execute(f"PRAGMA table_info({table});" if target_type == "sqlite" else f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}';")
            columns = [info[1] for info in cursor.fetchall()]

            # Construct column and placeholder strings
            col_string = ",".join(columns)
            placeholders = ",".join(["%s"] * len(columns)) if target_type in ["postgres", "mysql"] else ",".join(["?"] * len(columns))

            sql = f"INSERT INTO {table} ({col_string}) VALUES ({placeholders})"
            for row in rows:
                cursor.execute(sql, row)

        target_conn.commit()

    except Exception as e:
        # Roll in case of an error and raise the exception
        target_conn.rollback()
        logging.error(f"Error importing data into table {table}: {e}")
        raise

    finally:
        cursor.close()





