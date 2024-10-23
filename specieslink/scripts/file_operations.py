import json
import pandas as pd
from data_processing import DataProcessor

class FileReader:
    """
    Classe responsável por ler arquivos em diferentes formatos, como JSON e CSV.
    """
    
    @staticmethod
    def read_json(file_path):
        """Lê um arquivo JSON e retorna os dados como dicionário."""
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            print(f"Erro ao ler o arquivo JSON: {e}")
            return None

    @staticmethod
    def read_csv(file_path):
        """Lê um arquivo CSV e retorna um DataFrame do Pandas."""
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return None

    @classmethod
    def read_data(cls, file_path, file_type):
        """Lê arquivos com base no tipo (JSON ou CSV) e retorna os dados."""
        
        if file_type == 'json':
            return cls.read_json(file_path)
        elif file_type == 'csv':
            return cls.read_csv(file_path)
        else:
            print(f"Tipo de arquivo {file_type} não suportado.")
            return None

        
class FileOperations:
    
    @classmethod
    def process_data(cls, path_json, file_type):

        """
        Lê os dados de um arquivo (JSON ou CSV) e executa as transformações de dados.
        """
        
        raw_data = cls.read_data(path_json, file_type)

        # Usando os métodos da classe DataProcessor para processar os dados
        raw_data = DataProcessor.remove_type_key(raw_data)
        raw_data = DataProcessor.convert_coordinates_to_string(raw_data)
        cleaned_data = DataProcessor.process_datecollected(raw_data)

        return cleaned_data