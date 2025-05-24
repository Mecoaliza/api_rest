from transform import FileReader, DataProcessor
from extract import extract_all_data, save_raw_data
from load import load_data_to_mongo
import pandas as pd
import json
import os

PATH_JSON = '../data_raw/all_records.json'

PATH_CSV = '../data_processed/df_csv_tratado.csv'


def main():

    print("Extraindo dados...")

    data = extract_all_data()
    save_raw_data(data)


    print("Transformando dados...")

    raw_data = FileReader.read_data(PATH_JSON, 'json')

    if raw_data:
        raw_data = DataProcessor.remove_type_key(raw_data)
        raw_data = DataProcessor.convert_coordinates_to_string(raw_data)
        raw_data = DataProcessor.process_datecollected(raw_data)
        raw_data = DataProcessor.update_structure(raw_data)


        df = DataProcessor.json_to_dataframe(raw_data)

        nulls_info, median_nulls, columns_to_remove = DataProcessor.get_sparse_columns_info(df)
        df = DataProcessor.remove_sparse_columns(df, columns_to_remove)
        df = DataProcessor.fill_missing_values(df)

        df.to_csv(PATH_CSV, index=False)

        print("Carregando dados no MongoDB...")

        load_data_to_mongo(df)

        print("Pipeline ETL finalizado com sucesso!")
    else:
        print("Erro ao carregar os dados.")

if __name__ == "__main__":
    main()