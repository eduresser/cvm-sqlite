import os
import pandas as pd
from tqdm import tqdm
from typing import List, Dict, Tuple, Any
from .database import Database
from .file_manager import FileManager
from .utils import associate_tables_and_schemas, create_df_and_fit_to_schema, create_table_query, extract_table_name_from_file, extract_table_name_from_schema, get_files

CVM_PREFIX = 'https://dados.cvm.gov.br/dados/'

class CVMDataProcessor:
    def __init__(self, db_path: str, cvm_url: str = CVM_PREFIX + 'CIA_ABERTA', verbose: bool = False):
        self.db_path = db_path
        self.cvm_url = cvm_url + '/' if not cvm_url.endswith('/') else cvm_url
        self.verbose = verbose

        self.db = Database(db_path, self.verbose)
        self.file_manager = None

    def run(self) -> None:
        if not self.cvm_url.startswith(CVM_PREFIX):
            print(f"Error: The URL '{self.cvm_url}' does not belong to a CVM Data directory. The URL must start with '{CVM_PREFIX}'")
            return
        
        self._handle_database()
        self.db._disconnect()

    def query(self, query: str) -> List[Tuple[Any, ...]]:
        self.db._connect()
        try:
            result = self.db.query(query)
            return result
        except Exception as e:
            print(str(e))
            return []
        finally:
            self.db._disconnect()

    def _handle_database(self) -> None:
        print(f'Creating or updating {self.db_path}...\n')
        df_files = self._get_new_or_upgradable_files()

        if df_files.shape[0] > 0:
            self.file_manager = FileManager()

            try:
                self.db._delete_existing_records(df_files, 'files', 'name')
                self.db._insert_dataframe(df_files, 'files')
            except:
                self.db._create_files_table(df_files)

            self._process_files(df_files)
            self.file_manager.cleanup(remove_temp_dir=True)
            print(f'\n{self.db_path} is up to date.')
        else:
            print('Nothing to update.')

    def _get_new_or_upgradable_files(self) -> pd.DataFrame:
        new_df_files = get_files(self.cvm_url)
        try:
            current_db_files = self.db.query("SELECT * FROM files")
            current_df_files = pd.DataFrame(current_db_files, columns=new_df_files.columns)
            current_df_files['last_update'] = pd.to_datetime(current_df_files['last_update'])

            diff_df = new_df_files[new_df_files.apply(lambda row: self._row_diff(row, current_df_files), axis=1)]
            pending_df = current_df_files[current_df_files['status'] == 'PENDING']
            result_df = pd.concat([diff_df, pending_df], ignore_index=True)
            return result_df.drop_duplicates(keep='first')
        except:
            return new_df_files

    def _row_diff(self, new_row: pd.Series, current_df: pd.DataFrame) -> bool:
        if new_row['url'] not in current_df['url'].values: return True
        current_row = current_df[current_df['url'] == new_row['url']]
        return current_row['last_update'].iloc[0] != new_row['last_update']
    
    def _fetch_urls_by_category_and_file_type(self, category: str, file_type: str) -> List[str]:
        try:
            results = self.db.query(f"SELECT url FROM files WHERE category = ? AND type = ?", (category, file_type))
            return [result[0] for result in results] if results else []
        except Exception:
            return []

    def _process_files(self, df_files: pd.DataFrame) -> None:
        for category in df_files['category'].unique():
            df_category = df_files[df_files['category'] == category]
            meta = df_category.loc[df_category['type'] == 'META', 'url'].tolist()
            dados = df_category.loc[df_category['type'] == 'DADOS', 'url'].tolist()

            if not meta: meta = self._fetch_urls_by_category_and_file_type(category, 'META')
            if not dados: dados = self._fetch_urls_by_category_and_file_type(category, 'DADOS')

            if meta and dados:
                schema_files = self._download_schema_files(meta)
                self._process_data_files(dados, schema_files)
                self.file_manager.cleanup()
                self.db._update_files_status(dados + meta, 'url', 'COMPLETE')

    def _download_schema_files(self, meta_urls: List[str]) -> List[str]:
        schema_files = []
        for schema_url in meta_urls:
            schema_files.extend(self._download_and_extract(schema_url))
        return schema_files

    def _process_data_files(self, data_urls: List[str], schema_files: List[str]) -> None:
        tqdm_desc = f'Processing {extract_table_name_from_file(data_urls[0])} files'
        for data_url in tqdm(data_urls, disable=self.verbose, desc=tqdm_desc):
            table_files = self._download_and_extract(data_url)
            tables_and_schemas = associate_tables_and_schemas(table_files, schema_files)
            for table_and_schema in tables_and_schemas:
                self._process_table(table_and_schema)

    def _download_and_extract(self, url: str) -> List[str]:
        file_path = self.file_manager.download_file(url)
        if not file_path: return []
        if file_path.lower().endswith('.zip'):
            extracted_files = self.file_manager.unzip_file(file_path)
            self.file_manager.delete_file(file_path)
            return extracted_files
        return [file_path]

    def _process_table(self, table_and_schema: Dict[str, str]) -> None:
        table = table_and_schema['table']
        schema = create_table_query(table_and_schema['schema'])
        table_name = extract_table_name_from_schema(schema)
        if self.verbose: print(f"\nInserting data from '{os.path.basename(table)}'.")
        df = create_df_and_fit_to_schema(table, schema)
        self.db._create_table_if_not_exists(table_name, schema)
        self.db._delete_existing_records(df, table_name, 'source_file')
        self.db._insert_dataframe(df, table_name)
        self.file_manager.delete_file(table)