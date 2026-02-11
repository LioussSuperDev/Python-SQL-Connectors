from liouss_python_toolkit.printer import beautiful_print, RED_COLOR

from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, Tuple



class DatabaseNotConnectedError(Exception):
    """Erreur levée si une opération est effectuée sans connexion ouverte."""
    pass


class SQLConnection(ABC):
    """Classe abstraite définissant l’interface commune."""
    
    @abstractmethod
    def get_username(self) -> str:
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Vérifier si la connexion à la base de données est ouverte."""
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abstractmethod
    def get_db(self):
        """Retourner la connection à la base de données."""
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abstractmethod
    def connect(self):
        """Ouvrir la connexion."""
        raise NotImplementedError("This method should be implemented by subclasses")

    @abstractmethod
    def _query_one(self, request: str, *params, buffer_size:int=10000, **params2) -> List[Tuple]:
        """(Interne) Exécuter une requête et retourner les résultats."""
        raise NotImplementedError("This method should be implemented by subclasses")

    @abstractmethod
    def _query_many(self, request: str, *params, buffer_size:int=10000, batch_error=False, **params2) -> List[Tuple]:
        """(Interne) Exécuter une requête et retourner plusieurs résultats."""
        raise NotImplementedError("This method should be implemented by subclasses")

    @abstractmethod
    def close(self):
        """Fermer la connexion."""
        raise NotImplementedError("This method should be implemented by subclasses")
    
    def run_script(self, script_url:str) -> None:
        """Exécuter un script SQL à partir d’un fichier."""
        with open(script_url, "r", encoding="utf-8") as f:
            
            sql_script = f.read()

            # Exécuter le script complet
            for statement in sql_script.split(";"):
                stmt = statement.strip()
                if stmt:  # évite d'exécuter des requêtes vides
                    self.query_one(stmt)
            
            self.query_one("COMMIT")

    
    def query_one(self,
                  request: str,
                  *params,
                  try_nb:int=1,
                  print_error:bool=True,
                  ignore_errors:bool=False,
                  buffer_size:int=10000, 
                  **params2) -> Optional[List[Tuple]]:
        
        if not self.is_connected():
            raise DatabaseNotConnectedError("No open connection to database")
        
        try:
            return self._query_one(request, *params, buffer_size=buffer_size, **params2)
        except Exception as e:
            if try_nb<2:
                if ignore_errors:
                    return None
                else:
                    raise e
            elif print_error:
                beautiful_print("ERROR: executing database query",color=RED_COLOR)
                beautiful_print(f"Request failed: {request} with params {params}")
                beautiful_print(e,color=RED_COLOR)
                beautiful_print()
            return self.query_one(request, try_nb=try_nb-1, buffer_size=buffer_size, print_error=False, *params, **params2)

    def query_many(self,
                   request: str,
                   *params,
                   try_nb:int=1,
                   print_error:bool=True,
                   ignore_errors:bool=True,
                   buffer_size:int=10000, 
                   batch_error_lambda:Optional[Callable[[Any], None]]=None,
                   **params2) -> Optional[List[Tuple]]:
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