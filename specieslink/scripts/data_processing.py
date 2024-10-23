import pandas as pd

class DataProcessor:
    """
    Classe responsável por processar e transformar dados de um dicionário ou DataFrame.
    """

    @staticmethod
    def remove_type_key(data):
        """Remove a chave 'type' dos registros."""
        for record in data:
            if 'type' in record:
                del record['type']
            if 'geometry' in record and 'type' in record['geometry']:
                del record['geometry']['type']
        return data

    @staticmethod
    def convert_coordinates_to_string(data):
        """Converte as coordenadas da chave 'geometry' para string."""
        for record in data:
            if 'geometry' in record and 'coordinates' in record['geometry']:
                coordinates = record['geometry']['coordinates']
                if isinstance(coordinates, list):
                    record['geometry']['coordinates'] = ', '.join(map(str, coordinates))
        return data

    @staticmethod
    def process_datecollected(data):
        """Processa e une os campos de data 'daycollected', 'monthcollected' e 'yearcollected'."""
        for record in data:
            properties = record.get('properties', {})
            day = properties.get('daycollected')
            month = properties.get('monthcollected')
            year = properties.get('yearcollected')

            date_parts = [day.zfill(2) if day else '', month.zfill(2) if month else '', year]
            datecollected = '-'.join(part for part in date_parts if part)

            if datecollected:
                properties['datecollected'] = datecollected
                if 'daycollected' in properties:
                    del properties['daycollected']
                if 'monthcollected' in properties:
                    del properties['monthcollected']
                if 'yearcollected' in properties:
                    del properties['yearcollected']
        return data

    @staticmethod
    def json_to_dataframe(data):
        """Converte o JSON processado em um DataFrame."""
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
        """Preenche valores ausentes no DataFrame com 'Não informado'."""
        return df.fillna('Não informado')

    @staticmethod
    def get_sparse_columns_info(df):
        """Identifica colunas esparsas (com muitos valores nulos) e retorna informações sobre elas."""
        null_counts = df.isnull().sum()
        ordered_nulls = null_counts.sort_values(ascending=False)
        median_nulls = ordered_nulls.median()

        columns_to_remove = ordered_nulls[ordered_nulls > median_nulls].index.tolist()
        return ordered_nulls, median_nulls, columns_to_remove

    @staticmethod
    def remove_sparse_columns(df, columns_to_remove):
        """Remove as colunas esparsas identificadas."""
        if not columns_to_remove:
            print("Nenhuma coluna esparsa a ser removida.")
            return df
        return df.drop(columns=columns_to_remove)

    @staticmethod
    def update_structure(data):
        """Atualiza a estrutura dos registros movendo 'coordinates' de 'geometry' para 'properties'."""
        for record in data:
            geometry = record.get('geometry', {})
            coordinates = geometry.get('coordinates')

            if coordinates:
                properties = record.get('properties', {})
                properties['coordinates'] = coordinates
                if 'geometry' in record:
                    del record['geometry']
        return data
