import json
import pandas as pd

class FileReader:

    
    @staticmethod
    def read_json(file_path):

        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            print(f"Erro ao ler o arquivo JSON: {e}")
            return None

    @staticmethod
    def read_csv(file_path):

        try:
            return pd.read_csv(file_path)
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return None

    @classmethod
    def read_data(cls, file_path, file_type):

        
        if file_type == 'json':
            return cls.read_json(file_path)
        elif file_type == 'csv':
            return cls.read_csv(file_path)
        else:
            print(f"Tipo de arquivo {file_type} não suportado.")
            return None

class DataProcessor:


    @staticmethod
    def remove_type_key(data):

        for record in data:
            if 'type' in record:
                del record['type']
            if 'geometry' in record and 'type' in record['geometry']:
                del record['geometry']['type']
        return data

    @staticmethod
    def convert_coordinates_to_string(data):

        for record in data:
            if 'geometry' in record and 'coordinates' in record['geometry']:
                coordinates = record['geometry']['coordinates']
                if isinstance(coordinates, list):
                    record['geometry']['coordinates'] = ', '.join(map(str, coordinates))
        return data

    @staticmethod
    def process_datecollected(data):

        props = record.get('porperties', {})

        for record in data:

            day = props.pop('daycollected', '')
            month = props.pop('monthcollected', '')
            year = props.pop('yearcollected', '')

            date_parts = [day.zfill(2) if day else '', month.zfill(2) if month else '', year]
            datecollected = '-'.join(part for part in date_parts if part)

            if datecollected:
                props['datecollected'] = datecollected
                
        return data

    @staticmethod
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
    
    @staticmethod
    def json_to_dataframe(data):

        data_coordinates = []
        data_properties = []

        for record in data:
            data_coordinates.append(record.get('geometry', {}))
            data_properties.append(record.get('properties', {}))

        df_coordinates = pd.DataFrame(data_coordinates)
        df_properties = pd.DataFrame(data_properties)

        df_combined = pd.concat([df_coordinates, df_properties], axis=1)
        return df_combined

    @staticmethod
    def fill_missing_values(df):

        return df.fillna('Não informado')

    @staticmethod
    def get_sparse_columns_info(df):

        null_counts = df.isnull().sum()
        ordered_nulls = null_counts.sort_values(ascending=False)
        median_nulls = ordered_nulls.median()

        columns_to_remove = ordered_nulls[ordered_nulls > median_nulls].index.tolist()
        return ordered_nulls, median_nulls, columns_to_remove

    @staticmethod
    def remove_sparse_columns(df, columns_to_remove):

        if not columns_to_remove:
            print("Nenhuma coluna esparsa a ser removida.")
            return df
        return df.drop(columns=columns_to_remove)         