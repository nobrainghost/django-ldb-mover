##currently used as test file for the functions in utils.py
#Main will be here

from utils import connect_local_sqlitedb, export_sqlite_to_json, connect_to_postgresdb, load_connection_details, import_json_to_target, connect_local_sqlitedb, export_sqlite_to_json, connect_to_postgresdb, load_connection_details, import_json_to_target
from utils import check_data_transfer
local_db_file = 'test.sqlite3'
conn = connect_local_sqlitedb(local_db_file)

export_sqlite_to_json(conn, 'test.json')
postgres_connection_details = load_connection_details('conn_details.json')
conn = connect_to_postgresdb(postgres_connection_details)
# import_json_to_target(conn, 'postgres', 'test.json')
print(check_data_transfer(conn, 'users'))
