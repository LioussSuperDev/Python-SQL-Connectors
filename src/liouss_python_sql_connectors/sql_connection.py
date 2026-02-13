from abc import abstractmethod
from liouss_python_toolkit.printer import beautiful_print, RED_COLOR
from .db_connection import DBConnection, DatabaseNotConnectedError
import sqlparse

from typing import Any, Callable, List, Optional, Tuple


class SQLConnection(DBConnection):
    """Abstract class for SQL Connection (inherites DBConnection)"""
    
    def run_script(self, script_url:str) -> None:
        """Runs SQL script %to be improved"""
        with open(script_url, "r", encoding="utf-8") as f:
            
            sql_script = f.read()

            for statement in sqlparse.split(sql_script):
                stmt = statement.strip("; \n\r")
                if stmt:
                    self.query_one(stmt)
            
            self.query_one("COMMIT")
    
    @abstractmethod
    def _query_many(self, request: str, *params, buffer_size:int=10000, batch_error_lambda=None, **params2) -> List[Tuple]:
        """INTERNAL/DO NOT CALL OR YOU ARE FIRED\n
        Executes a DML query on the SGBD. for every tuples in *params. Returns the returned result\n
        batch_error_lambda = Lambda to apply when error is raised on one of the queries\n"""
        raise NotImplementedError("This method should be implemented by subclasses")

    def query_many(self,
                   request: str,
                   *params,
                   try_nb:int=1,
                   print_error:bool=True,
                   ignore_errors:bool=True,
                   buffer_size:int=10000, 
                   batch_error_lambda:Optional[Callable[[Any], None]]=None,
                   **params2) -> Optional[List[Tuple]]:
        """Executes a DML query on the SGBD. for every tuples in *params. Returns the returned result\n
        try_nb:int = Number of retries when the query fails\n
        print_error:bool = prints global raised (not per query) errors if True\n
        buffer_size:int = number of elements to retrieve at one time\n
        batch_error_lambda = Lambda to apply when error is raised on one of the queries\n"""
        if not self.is_connected():
            raise DatabaseNotConnectedError("No open connection to database")
        
        try:
            return self._query_many(request, *params, buffer_size=buffer_size, batch_error_lambda=batch_error_lambda, **params2)
        except Exception as e:
            if try_nb<2:
                if ignore_errors:
                    return None
                else:
                    raise e
            elif print_error:
                beautiful_print("ERROR: executing database query_many",color=RED_COLOR)
                beautiful_print(f"Request failed: {request} with params {str(params)[:1000]}")
                beautiful_print(e,color=RED_COLOR)
                beautiful_print()
            return self.query_many(request, try_nb=try_nb-1, buffer_size=buffer_size, *params, **params2)


    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
