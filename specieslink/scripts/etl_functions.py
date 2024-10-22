import json
import pandas as pd

# Caminho do arquivo JSON
PATH_JSON = '../data_raw/all_records.json'

# 1. Funções de leitura de dados

def read_json(path_json):

    """Lê um arquivo JSON e retorna os dados carregados."""

    with open(path_json, 'r') as file:
        return json.load(file)
    

def read_data(path, file_type):
    """Lê dados a partir de diferentes tipos de arquivos."""
    if file_type == 'json':
        return read_json(path)
    # Você pode adicionar mais tipos de arquivos se necessário
    return []

# 2. Funções de processamento de dados

def remove_type_key(data):
    """Remove as chaves 'type' de cada registro."""
    for record in data:
        record.pop('type', None)
        if 'geometry' in record:
            record['geometry'].pop('type', None)
    return data

def convert_coordinates_to_string(data):
    """Converte as coordenadas de uma lista para uma string."""
    for record in data:  
        if 'geometry' in record and 'coordinates' in record['geometry']:
            coordinates = record['geometry']['coordinates']
            if isinstance(coordinates, list):
                record['geometry']['coordinates'] = ', '.join(map(str, coordinates))
    return data

def process_datecollected(data):
    """Combina os campos de coleta de data (dia, mês, ano) em um único campo."""
    for record in data:
        properties = record.get('properties', {})
        day = properties.get('daycollected')
        month = properties.get('monthcollected')
        year = properties.get('yearcollected')
        
        date_parts = [day.zfill(2) if day else '', month.zfill(2) if month else '', year]
        datecollected = '-'.join(part for part in date_parts if part)

        if datecollected:
            properties['datecollected'] = datecollected
            for key in ['daycollected', 'monthcollected', 'yearcollected']:
                properties.pop(key, None)
    return data

def update_structure(data):
    """Move as coordenadas para dentro de 'properties' e remove a chave 'geometry'."""
    for record in data:
        geometry = record.get('geometry', {})
        coordinates = geometry.get('coordinates')

        if coordinates:
            record.setdefault('properties', {})['coordinates'] = coordinates
            record.pop('geometry', None)
    return data

# 3. Funções de manipulação de DataFrame

def json_to_dataframe(data):
    """Converte dados JSON em um DataFrame do Pandas."""
    coordinates = [record['geometry'] for record in data if 'geometry' in record]
    properties = [record['properties'] for record in data if 'properties' in record]

    df_coordinates = pd.DataFrame(coordinates)
    df_properties = pd.DataFrame(properties)
    
    return pd.concat([df_coordinates, df_properties], axis=1)

def fill_missing_values(df):
    """Preenche valores nulos com 'Não informado'."""
    return df.fillna('Não informado')

def get_sparse_columns_info(df):

    """Retorna informações sobre colunas esparsas e as que precisam ser removidas."""

    null_counts = df.isnull().sum()
    ordered_nulls = null_counts.sort_values(ascending=False)
    median_nulls = ordered_nulls.median()

    columns_to_remove = ordered_nulls[ordered_nulls > median_nulls].index
    return ordered_nulls, median_nulls, columns_to_remove

def remove_sparse_columns(df, columns_to_remove):

    """Remove as colunas esparsas do DataFrame."""

    return df.drop(columns=columns_to_remove)

# 4. Funções de pipeline de dados

def process_data(path_json, file_type):
    """Executa o pipeline de processamento completo dos dados."""
    raw_data = read_data(path_json, file_type)
    raw_data = remove_type_key(raw_data)
    raw_data = convert_coordinates_to_string(raw_data)
    raw_data = process_datecollected(raw_data)
    return update_structure(raw_data)

def save_df_to_csv(df, path_csv):
    """Salva o DataFrame como CSV."""
    df.to_csv(path_csv, index=False)

# 5. Main script de execução

if __name__ == "__main__":
    # Carregando e processando os dados
    raw_data = read_data(PATH_JSON, 'json')

    processed_data = process_data(PATH_JSON, 'json')


    # Transformando os dados em DataFrame
    df = json_to_dataframe(processed_data)

    # Identificando e removendo colunas esparsas

    columns_null_ordenadas, media, columns_to_remove = get_sparse_columns_info(df)

    df_remove_space = remove_sparse_columns(df, columns_to_remove)

    # Preenchendo valores nulos
    df_cleaned = fill_missing_values(df_remove_space)

   

    # Salvando os dados processados em CSV
    save_df_to_csv(df_cleaned, '../data_processed/df_csv_tratado.csv')

    # Exibindo uma amostra dos dados processados
    print(df_cleaned.head())
