from typing import Any, Callable, List, Optional, Tuple, cast
from .sql_connection import SQLConnection
import sqlite3
import datetime
import os


class SQLiteConnection(SQLConnection):
    def __init__(self, db_path: str, logs: bool = False):
        self.config = {
            "db_path": db_path
        }
        self.connection: Optional[sqlite3.Connection] = None
        self.activated_logs = logs
        self.logs = []

    def get_username(self) -> str:
        return "sqlite"

    def is_connected(self) -> bool:
        return self.connection is not None

    def get_db(self) -> sqlite3.Connection:
        if not self.is_connected():
            raise ConnectionError("No open connection to SQLite database")
        return cast(sqlite3.Connection, self.connection)

    def connect(self, dml_parallel: bool = True):
        try:
            db_path = self.config["db_path"]

            parent_dir = os.path.dirname(os.path.abspath(db_path))
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)

            # Crée automatiquement la base si elle n'existe pas
            self.connection = sqlite3.connect(db_path)

        except sqlite3.Error as e:
            raise ConnectionError("Unable to connect SQLite database") from e

    def _query_one(
        self,
        query: str,
        *params,
        buffer_size: int = 10000,
        include_col_name: bool = False,
        **params2
    ) -> List[Tuple]:

        if self.activated_logs:
            self.logs.append(f"[{str(datetime.datetime.now())}]\n")
            self.logs.append(f"{query}; [{str(params)[:500]}]\n\n")


        cur = self.get_db().cursor()
        try:
            if params2:
                cur.execute(query, params2)
            elif params:
                if len(params) == 1 and isinstance(params[0], (tuple, list, dict)):
                    cur.execute(query, params[0])
                else:
                    cur.execute(query, params)
            else:
                cur.execute(query)

            if include_col_name and cur.description:
                col_names = tuple(col[0] for col in cur.description)
                rows = cur.fetchall()
                return [col_names] + rows
            else:
                return cur.fetchall()

        except sqlite3.Error as e:
            raise e
        finally:
            cur.close()

    def _query_many(
        self,
        query: str,
        *params,
        buffer_size: int = 10000,
        batch_error_lambda: Optional[Callable[[Any], None]] = None,
        **params2
    ) -> List[Tuple]:

        if self.activated_logs:
            self.logs.append(f"[{str(datetime.datetime.now())} MANY]\n")
            self.logs.append(f"{query}; [{str(params)[:500]}]\n\n")

        cur = self.get_db().cursor()
        try:
            if not params:
                raise ValueError("executemany requires a sequence of parameter sets")

            param_sets = params[0]

            if batch_error_lambda is None:
                cur.executemany(query, param_sets)
            else:
                for param_set in param_sets:
                    try:
                        cur.execute(query, param_set)
                    except sqlite3.Error as e:
                        batch_error_lambda(e)

            try:
                return cur.fetchall()
            except sqlite3.Error:
                return []

        finally:
            cur.close()

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

        if self.activated_logs:
            logs_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "logs"
            )
            os.makedirs(logs_dir, exist_ok=True)

            logs_path = os.path.join(
                logs_dir,
                f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_logs.txt"
            )

            with open(logs_path, "w+", encoding="utf-8") as f:
                f.writelines(self.logs)