from typing import Optional
from .sql_connection import SQLConnection
from .oracle_connection import OracleConnection
from .sqlite_connection import SQLiteConnection
def generateConnection(connection_type:str, identifiers:dict) -> Optional[SQLConnection]:
    
    if connection_type == "oracle":
        return OracleConnection(
            identifiers["username"],
            identifiers["password"],
            identifiers["hostname"],
            identifiers["service_name"],
            logs = False
        )
    if connection_type == "sqlite":
        return SQLiteConnection(identifiers["db_path"])
    return None