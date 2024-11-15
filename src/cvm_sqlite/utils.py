import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Tuple
from urllib.parse import urljoin

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

    def get_date_by_item(item) -> datetime:
        try:
            date_size = item.next_sibling.strip().split()
            date_str = ' '.join(date_size[:2])
            return datetime.strptime(date_str, "%d-%b-%Y %H:%M")
        except ValueError:
            return None

    def map_directory(url: str) -> Tuple[List[Dict], List[str]]:
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find('pre').find_all('a')
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
            return files, folders
        except:
            return [], []

    def map_files(url: str) -> List[Dict]:
        all_files = []
        files, folders = map_directory(url)
        all_files.extend(files)
        for folder in folders:
            all_files.extend(map_files(folder))
        return all_files

    files = map_files(url)
    return pd.DataFrame(files)

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
                if set(entry) != set('-'):
                    key, value = entry.split(':')
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

def create_df_and_fit_to_schema(table_path: str, create_table_query: str) -> pd.DataFrame:
    df = pd.read_csv(table_path, encoding='ISO-8859-1', sep=';', quoting=3, on_bad_lines='skip', low_memory=False)
    df['source_file'] = table_path.split('/')[-1]

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
                df[col] = df[col].where(pd.notnull(df[col]), None)
                df[col] = df[col].where(df[col] != 'None', None)

    for col, type_def in column_defs:
        if 'CHAR' in type_def or 'VARCHAR' in type_def:
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) and x is not None else None)
            match = re.search(r'\((\d+)\)', type_def)
            if match:
                max_length = int(match.group(1))
                df[col] = df[col].apply(lambda x: x[:max_length] if x is not None else None)

    return df

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