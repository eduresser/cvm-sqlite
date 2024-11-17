import os
import pandas as pd
import sqlite3
from typing import List, Tuple, Any, Optional

class Database:
    def __init__(self, db_path: str, verbose: bool = False):
        directory = os.path.dirname(db_path)
        if directory != '' and not os.path.exists(directory): os.makedirs(directory)

        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.verbose = verbose
        self._connect()

    def _connect(self) -> None:
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()

    def _disconnect(self) -> None:
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()
        self.conn = None
        self.cursor = None

    def _create_table_if_not_exists(self, table_name: str, create_table_query: str) -> None:
        table_exists = self.query(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not table_exists:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            if self.verbose: print(f"Table '{table_name}' created successfully.")

    def _delete_existing_records(self, df: pd.DataFrame, table_name: str, column: str) -> int:
        unique_values = df[column].unique()
        placeholders = ','.join(['?' for _ in unique_values])
        delete_query = f"DELETE FROM {table_name} WHERE {column} IN ({placeholders})"
        self.query(delete_query, tuple(unique_values))
        affected_rows = self.cursor.rowcount
        self.conn.commit()
        if self.verbose and affected_rows > 0:
            files = f"'{unique_values[0]}'." if len(unique_values) < 2 else f"\n - {'\n - '.join(map(str, unique_values))}\n"
            print(f"{affected_rows} old records were deleted related to file(s): {files}")
        return affected_rows

    def _insert_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        df.to_sql(table_name, self.conn, if_exists='append', index=False)
        if self.verbose: print(f"{df.shape[0]} new records were inserted into table '{table_name}'.")

    def _create_files_table(self, df_files: pd.DataFrame) -> None:
        df_files.to_sql('files', self.conn, if_exists='replace', index=False)
        print(f"{df_files.shape[0]} files founded in the given CVM directory.")

    def _update_files_status(self, names: List[str], column: str, new_status: str) -> None:
        formatted_names = ", ".join([f"'{name}'" for name in names])
        try:
            update_query = f"UPDATE files SET status = '{new_status}' WHERE {column} IN ({formatted_names})"
            self.query(update_query)
            self.conn.commit()
        except Exception as e:
            print(f"Error updating status of files {formatted_names} in the database: {str(e)}")

    def query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]:
        self._connect()
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()