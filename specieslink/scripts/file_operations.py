import json
import pandas as pd

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

    @staticmethod
    def read_data(file_path, file_type):
        
        """Lê arquivos com base no tipo (JSON ou CSV) e retorna os dados."""
        if file_type == 'json':
            return FileReader.read_json(file_path)
        elif file_type == 'csv':
            return FileReader.read_csv(file_path)
        else:
            print(f"Tipo de arquivo {file_type} não suportado.")
            return None
