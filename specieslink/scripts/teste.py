import json
import pandas as pd

path_json = '../data_raw/all_records.json'

# 1. Iniciando a leitura dos dados

def read_json(path_json):
    data = []
    with open(path_json, 'r') as file:
        data = json.load(file)
    return data

# Função para casos que tenha arquivos de origens diferentes
def read_datas(path, type_file):
    dados = []

    if type_file == 'csv':
        dados = read_json(path)
    elif type_file == 'json':
        dados = read_json(path)

    return dados

def get_columns(dados):
    return list(dados[0].keys())

# Transformação dos dados

# 2. Tratando dados

def remove_type_key(dados):
    for record in dados:
        if 'type' in record:
            del record['type']

        if 'geometry' in record and 'type' in record['geometry']:
            del record['geometry']['type']

    return dados


def convert_coordinates_to_string(dados):
    for record in dados:  
        if 'geometry' in record and 'coordinates' in record['geometry']:
            coordinates = record['geometry']['coordinates']
            if isinstance(coordinates, list):
                record['geometry']['coordinates'] = ', '.join(map(str, coordinates))
    return dados


def process_datecollected(dados):
    for record in dados:
        properties = record.get('properties', {})
        day = properties.get('daycollected')
        month = properties.get('monthcollected')
        year = properties.get('yearcollected')
        
        date_parts = [day.zfill(2) if day else '', month.zfill(2) if month else '', year]
        datecollected = '-'.join(part for part in date_parts if part)

        if datecollected:
            properties['datecollected'] = datecollected
            # Remover os campos antigos
            if 'daycollected' in properties:
                del properties['daycollected']
            if 'monthcollected' in properties:
                del properties['monthcollected']
            if 'yearcollected' in properties:
                del properties['yearcollected']
    
    return dados

def process_data(path_json, type_file):
    raw_data = read_datas(path_json, type_file)

    cleaned_data = remove_type_key(raw_data)
    cleaned_data = convert_coordinates_to_string(cleaned_data)
    cleaned_data = process_datecollected(cleaned_data)

    return cleaned_data

def size_data(dados):
    return len(dados)

def json_to_dataframe(dados):
    data_coordinates = []
    data_properties = []

    for record in dados:
        data_coordinates.append(record['geometry'])
        data_properties.append(record['properties'])

    
    df_cordenadas = pd.DataFrame(data_coordinates)
    df_properties = pd.DataFrame(data_properties)

    df_combined = pd.concat([df_cordenadas, df_properties], axis=1)

    return df_combined

def fill_missing_values(df):
    df_filled = df.fillna('Não informado')
    return  df_filled

def get_sparse_columns_info(df):
    columns_null = df.isnull().sum()
    columns_null_order = columns_null.sort_values(ascending=False)
    media = columns_null_order.median()

    columns_to_remove = columns_null_order[columns_null_order > media].index

    return columns_null_order, media, columns_to_remove


def remove_sparse_columns(df, columns_to_remove):
    df_cleaned = df.drop(columns=columns_to_remove)

    return df_cleaned

def update_structure(data):

    for record in data:

        geometry = record.get('geometry', {})
        coordinates = geometry.get('coordinates')

        if coordinates:
            properties = record.get('properties', {})

            properties['coordinates'] = coordinates

            if 'geometry' in record:
                del record['geometry']

    return data


def save_df_to_csv(df, path_csv):
    df.to_csv(path_csv, index=False)

# Iniciando a leitura dos dados

data = read_datas(path_json, 'json')
#print(data[0]['properties'])

# Resgatando o nome das colunas
collumns_names = get_columns(data)

# Excluindo valores com type, convertendo coordenadas para string, adicionando campo de data de coleta formatado
processed_data  = process_data(path_json, 'json')
#print(processed_data[0])

# Verificar tamanho da base de dados
tamanho_dados_iniciais = size_data(data)
#print(f"Json brutos da api com: {tamanho_dados_iniciais} registros.")

# Transformando o arquivo json em um dataframe
df = json_to_dataframe(processed_data)
#print(df.head())



# Seleciona as colunas que têm uma quantidade de valores nulos maior do que a mediana e retorna seus nomes.
columns_null_ordenadas, media, columns_to_remove = get_sparse_columns_info(df)
# print("Colunas vazias ordenadas:\n", columns_null_ordenadas)
# print("\nMediana dos valores nulos:", media)
# print("\nColunas a serem removidas:", columns_to_remove)


# Remove as colunas com muitos valores nulos do dataframe

df_cleaned = remove_sparse_columns(df, columns_to_remove)
#print("\nDataFrame após remover as colunas com muitos valores nulos:\n", df_cleaned.head(2))

# Tratamento de valores ausentes com valores padrão

df_cleaned_values = fill_missing_values(df_cleaned)
#print(null_values.head(5))




# EXTRACT

dataset = read_datas(path_json, 'json')
#print(dataset.data[0])

#print(dataset.nome_colunas)
#print(dataset.qtd_rows)


# TRANSFORM

dataset.remove_type_key()
#print(dataset.remove_type_key()[0])

dataset.convert_coordinates_to_string()
#print(dataset.convert_coordinates_to_string()[0])

dataset.process_datecollected()
#print(dataset.process_datecollected()[0])

processed = process_data(path_json, 'json')
#print(processed[0])

atualiza = update_structure(processed)
print(atualiza[0])
#print(dataset.size_data)
# print(type_value[0])


# LOAD

# Salvando os dados em CSV
df = df_cleaned_values.head(5)
print(df)

save_df_to_csv(df_cleaned, '../data_processed/df_csv_tratado.csv')