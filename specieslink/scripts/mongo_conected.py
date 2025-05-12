import pandas as pd
from pymongo import MongoClient

df = pd.read_csv('../data_processed/df_csv_tratado.csv')

client = MongoClient('mongodb://localhost:27017/')
db = client['species']
colecao = db['especies']

dados = df.to_dict(orient='records')

colecao.insert_many(dados)

print('Importação concluída com sucesso!')