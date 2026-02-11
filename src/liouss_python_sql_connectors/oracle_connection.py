from typing import Any, Callable, List, Optional, Tuple, cast
from src.liouss_python_sql_connectors.sql_connection import SQLConnection
import oracledb
import datetime
import os

class OracleConnection(SQLConnection):
    def __init__(self, user:str, password:str, host:str, service_name:str, logs:bool = False):
        self.config = {
            "user": user,
            "password": password,
            "host": host,
            "service_name": service_name
        }
        self.username = user
        self.connection : oracledb.Connection | None = None
        self.activated_logs = logs
        self.logs = []
    
    def get_username(self) -> str:
        return self.username
    
    def is_connected(self) -> bool:
        """Vérifier si la connexion à la base de données est ouverte."""
        return self.connection is not None

    def get_db(self) -> oracledb.Connection:
        """Retourner la connection à la base de données Oracle."""
        if self.is_connected() == False:
            raise ConnectionError("No open connection to Oracle database")
        return cast(oracledb.Connection, self.connection)

    def connect(self, dml_parallel:bool=True):
        try:
            self.connection = oracledb.connect(user=self.config["user"],
                                        password=self.config["password"],
                                        host=self.config["host"],
                                        port=1521,
                                        service_name=self.config["service_name"])
            #TODO AJOUTER UN TRY
            if dml_parallel:
                self.query_one("ALTER SESSION ENABLE PARALLEL DML")
        except oracledb.DatabaseError as e:
            raise ConnectionError("Unable to connect Oracle database") from e

    def _query_one(self, request: str, *params, buffer_size:int=10000, **params2) -> List[Tuple]:
        """Exécuter une requête et retourner les résultats."""
        
        if self.activated_logs:
            self.logs.append(f"[{str(datetime.datetime.now())}]\n")
            self.logs.append(f"{request}; [{str(params)[:500]}]\n\n")
            
        with self.get_db().cursor() as cur:
            cur.arraysize = buffer_size
            cur.prefetchrows = buffer_size+1
            try:
                cur.execute(request, *params, **params2)
                return cur.fetchall()
            except oracledb.InterfaceError as e:
                if "DPY-1003" in str(e):
                    return []
                else:
                    raise e
                
    def _query_many(self, request: str, *params, buffer_size:int=10000, batch_error_lambda:Optional[Callable[[Any], None]]=None, **params2) -> List[Tuple]:
        
        if self.activated_logs:
            self.logs.append(f"[{str(datetime.datetime.now())} MANY]\n")
            self.logs.append(f"{request}; [{str(params)[:500]}]\n\n")
            
        """Exécuter une requête par abtch et retourner les résultats."""
        with self.get_db().cursor() as cur:
            cur.arraysize = buffer_size
            cur.prefetchrows = buffer_size+1
            cur.executemany(request, *params, **params2, batcherrors=batch_error_lambda!=None)
            if batch_error_lambda and len(cur.getbatcherrors()) > 0:
                for error in cur.getbatcherrors():
                    batch_error_lambda(error)
            try:
                return cur.fetchall()
            except oracledb.InterfaceError as e:
                if "DPY-1003" in str(e):
                    return []
                else:
                    raise e

    def close(self):
        """Fermer la connexion à la base de données Oracle."""
        if self.connection:
            self.connection.close()
            self.connection = None
        
        if self.activated_logs:
            logs_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                os.path.join("logs/",f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_logs.txt")
            )
            with open(logs_path,'w+') as f:
                f.writelines(self.logs)