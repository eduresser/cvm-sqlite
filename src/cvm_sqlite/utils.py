import gc
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Tuple, Iterator, Optional
from urllib.parse import urljoin

CSV_CHUNK_SIZE = 10000

def extract_table_name_from_file(string: str) -> str:
    condition = lambda x: x != 'cad' and x != 'meta' and x != 'txt' and not x.isnumeric()
    splitted_file_name = string.split('/')[-1].split('.')[0].split('_')
    filtered_splitted_file_name = [word for word in splitted_file_name if condition(word)]
    return '_'.join(filtered_splitted_file_name)

def extract_table_name_from_schema(schema: str) -> str:
    return re.search(r'CREATE TABLE (\w+)', schema).group(1)

def get_files(url: str) -> pd.DataFrame:
    def get_type_by_url(url: str) -> str:
        if '/DADOS/' in url: return 'DADOS'
        if '/META/' in url: return 'META'
        return None

    def get_date_by_item(item) -> Optional[datetime]:
        try:
            date_size = item.next_sibling.strip().split()
            date_str = ' '.join(date_size[:2])
            return datetime.strptime(date_str, "%d-%b-%Y %H:%M")
        except ValueError:
            return None

    def map_directory(url: str) -> Tuple[List[Dict], List[str]]:
        try:
            response = requests.get(url, timeout=30)
            soup = BeautifulSoup(response.text, 'lxml')
            pre_element = soup.find('pre')
            if not pre_element:
                return [], []
            items = pre_element.find_all('a')
            files = []
            folders = []
            for item in items[1:]:
                file_name = item.text.strip()
                item_url = urljoin(url, item['href'])
                if file_name.endswith('/'):
                    folders.append(item_url)
                else:
                    files.append({
                        'name': file_name,
                        'category': extract_table_name_from_file(file_name),
                        'type': get_type_by_url(url),
                        'url': item_url,
                        'last_update': get_date_by_item(item),
                        'status': 'PENDING'
                    })
            del soup, response
            return files, folders
        except:
            return [], []

    all_files = []
    folders_to_process = [url]
    
    while folders_to_process:
        current_url = folders_to_process.pop(0)
        files, subfolders = map_directory(current_url)
        all_files.extend(files)
        folders_to_process.extend(subfolders)
        del files, subfolders
    
    result = pd.DataFrame(all_files)
    del all_files
    gc.collect()
    return result

def create_table_query(schema_path: str) -> str:
    def read_text_file(path: str) -> str:
        try:
            with open(path, 'r', encoding='ISO-8859-1') as file:
                content = file.read()
            return content
        except FileNotFoundError:
            return f"Error: File '{path}' not found."
        except IOError:
            return f"Error: Problem reading file '{path}'."

    def struct_schema(meta_content: str) -> List[Dict]:
        fields = [field for field in meta_content.split('\n\n') if field != '']
        result = []
        for field in fields:
            entries = field.split('\n')
            field_dict = {}
            for entry in entries:
                if set(entry) != set('-') and ':' in entry:
                    key, value = entry.split(':', 1)
                    field_dict[key.strip()] = value.strip()
            result.append(field_dict)
        return result

    def generate_query(table_name: str, fields_list: List[Dict]) -> str:
        sql_parts = [f"CREATE TABLE {table_name} ("]
        for field in fields_list:
            field_name = field['Campo']
            data_type = field['Tipo Dados'].upper()
            if data_type in ('VARCHAR', 'CHAR'):
                size = field['Tamanho']
                sql_parts.append(f"    {field_name} {data_type}({size}),")
            elif data_type == 'DATE':
                sql_parts.append(f"    {field_name} DATE,")
            elif data_type == 'SMALLINT':
                precision = field.get('Precisão', '5')
                sql_parts.append(f"    {field_name} {data_type}({precision}),")
            elif data_type == 'DECIMAL':
                precision = field.get('Precisão', '10')
                scale = field.get('Scale', '0')
                sql_parts.append(f"    {field_name} {data_type}({precision},{scale}),")
            else:
                sql_parts.append(f"    {field_name} {data_type},")
        sql_parts.append("    source_file VARCHAR")
        sql_parts.append(");")
        return "\n".join(sql_parts)

    table_name = extract_table_name_from_file(schema_path)
    raw_schema = read_text_file(schema_path)
    structured_schema = struct_schema(raw_schema)
    return generate_query(table_name, structured_schema)

def _parse_column_defs(create_table_query: str) -> Tuple[List[Tuple[str, str]], Dict[str, str]]:
    """Extrai definições de colunas e tipos do schema."""
    column_defs = re.findall(r'(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)', create_table_query)[1:]
    
    column_types = {}
    for col, type_def in column_defs:
        if 'CHAR' in type_def or 'VARCHAR' in type_def:
            column_types[col] = 'object'
        elif 'INT' in type_def or 'SMALLINT' in type_def:
            column_types[col] = 'int64'
        elif 'DECIMAL' in type_def:
            column_types[col] = 'float64'
        elif 'DATE' in type_def:
            column_types[col] = 'datetime64[ns]'
        else:
            column_types[col] = 'object'
    
    return column_defs, column_types

def _is_nullable(x) -> bool:
    if x is None:
        return True
    if pd.isna(x):
        return True
    if isinstance(x, str) and x.lower() in ('nan', 'none', 'null', ''):
        return True
    return False

def _create_column_mapping(df_columns: List[str], schema_columns: List[str]) -> Dict[str, str]:
    mapping = {}
    df_cols_lower = {col.lower(): col for col in df_columns}
    
    for schema_col in schema_columns:
        if schema_col.lower() in df_cols_lower:
            mapping[df_cols_lower[schema_col.lower()]] = schema_col
    
    # Segundo, verifica se um está contido no outro
    remaining_df_cols = [col for col in df_columns if col not in mapping]
    remaining_schema_cols = [col for col in schema_columns if col not in mapping.values()]
    
    for df_col in remaining_df_cols:
        df_lower = df_col.lower()
        for schema_col in remaining_schema_cols:
            schema_lower = schema_col.lower()
            if df_lower in schema_lower or schema_lower in df_lower:
                if schema_col not in mapping.values():
                    mapping[df_col] = schema_col
                    break
    
    return mapping


def _fit_chunk_to_schema(df: pd.DataFrame, column_defs: List[Tuple[str, str]], column_types: Dict[str, str], source_file: str) -> pd.DataFrame:
    df['source_file'] = source_file
    
    schema_columns = list(column_types.keys())
    column_mapping = _create_column_mapping(list(df.columns), schema_columns)
    
    if column_mapping:
        df = df.rename(columns=column_mapping)

    for col in column_types:
        if col not in df.columns:
            df[col] = None

    df = df[[col for col in column_types.keys() if col in df.columns]]

    for col, dtype in column_types.items():
        if col in df.columns:
            if dtype == 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif dtype == 'int64':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif dtype == 'float64':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = df[col].apply(lambda x: None if _is_nullable(x) else x)

    for col, type_def in column_defs:
        if 'CHAR' in type_def or 'VARCHAR' in type_def:
            df[col] = df[col].apply(lambda x: None if _is_nullable(x) else str(x))
            match = re.search(r'\((\d+)\)', type_def)
            if match:
                max_length = int(match.group(1))
                df[col] = df[col].apply(lambda x: None if _is_nullable(x) else str(x)[:max_length])

    return df

def create_df_chunks_and_fit_to_schema(table_path: str, create_table_query: str) -> Iterator[pd.DataFrame]:
    column_defs, column_types = _parse_column_defs(create_table_query)
    source_file = table_path.split('/')[-1]
    
    for chunk in pd.read_csv(
        table_path, 
        encoding='ISO-8859-1', 
        sep=';', 
        quoting=3, 
        on_bad_lines='skip',
        chunksize=CSV_CHUNK_SIZE,
        low_memory=True
    ):
        yield _fit_chunk_to_schema(chunk, column_defs, column_types, source_file)
        gc.collect()


def create_df_and_fit_to_schema(table_path: str, create_table_query: str) -> pd.DataFrame:
    """Versão que retorna DataFrame completo (para compatibilidade). Use create_df_chunks_and_fit_to_schema para arquivos grandes."""
    chunks = list(create_df_chunks_and_fit_to_schema(table_path, create_table_query))
    if not chunks:
        column_defs, column_types = _parse_column_defs(create_table_query)
        return pd.DataFrame(columns=list(column_types.keys()))
    result = pd.concat(chunks, ignore_index=True)
    del chunks
    gc.collect()
    return result

def associate_tables_and_schemas(table_files: List[str], schema_files: List[str]) -> List[Dict[str, str]]:
    result = []
    schema_dict = {extract_table_name_from_file(file): file for file in schema_files}
    
    for table in table_files:
        table_base = extract_table_name_from_file(table)
        matching_schema = schema_dict.get(table_base)
        
        if not matching_schema:
            table_base = max([
                schema for schema in schema_dict.keys() if schema in table_base
            ], key=len, default=None)
            matching_schema = schema_dict.get(table_base)
        
        if matching_schema:
            result.append({
                'table': table,
                'schema': matching_schema
            })
    
    return result