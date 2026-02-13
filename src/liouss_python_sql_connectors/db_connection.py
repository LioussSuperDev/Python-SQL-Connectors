from liouss_python_toolkit.printer import beautiful_print, RED_COLOR

from abc import ABC, abstractmethod
from typing import Any

class DatabaseNotConnectedError(Exception):
    """Error raised if the database is not connected"""
    pass


class DBConnection(ABC):
    """Abstract class representing any database connection\n
    can : Connect / Query / Disconnect"""
    
    @abstractmethod
    def get_username(self) -> str:
        """Returns the connected username"""
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Verifies if the connection is connected"""
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abstractmethod
    def get_db(self):
        """Returns connection module to the database\n
        For instance : oracledb -> OracleConnection"""
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abstractmethod
    def connect(self):
        """Connect to the database"""
        raise NotImplementedError("This method should be implemented by subclasses")

    @abstractmethod
    def _query_one(self, query: str, *params, buffer_size:int=10000, col_name=False, **params2) -> Any:
        """INTERNAL/DO NOT CALL OR YOU ARE FIRED\n
        Executes the query on the SGBD. Returns the returned result\n
        col_name = True if you want to return columns names (for relational databases)"""
        raise NotImplementedError("This method should be implemented by subclasses")

    @abstractmethod
    def close(self):
        """Fermer la connexion."""
        raise NotImplementedError("This method should be implemented by subclasses")

    
    def query_one(self,
                  query: str,
                  *params,
                  try_nb:int=1,
                  print_error:bool=True,
                  ignore_errors:bool=False,
                  buffer_size:int=10000,
                  include_col_name:bool=False,
                  **params2) -> Any:
        """
        Queries the database\n
        try_nb:int -> number of tries if the query fails\n
        print_error:bool -> prints errors if any\n
        ignore_errors:bool -> no error raised if True\n
        buffer_size:int -> receiving buffer size\n
        include_col_name:bool -> For relational databases: True -> cols are returned as the first tuple.\n
        RETURNS: list of received elements
        """
    
    
        if not self.is_connected():
            raise DatabaseNotConnectedError("No open connection to database")
        
        try:
            return self._query_one(query, *params, buffer_size=buffer_size, include_col_name=include_col_name, **params2)
        except Exception as e:
            if try_nb<2:
                if ignore_errors:
                    return None
                else:
                    raise e
            elif print_error:
                beautiful_print("ERROR: executing database query",color=RED_COLOR)
                beautiful_print(f"Request failed: {query} with params {params}")
                beautiful_print(e,color=RED_COLOR)
                beautiful_print()
            return self.query_one(query, try_nb=try_nb-1, buffer_size=buffer_size, print_error=False, *params, **params2)


    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
