import os

from Core.constants import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

class PySparkRuneScape():
    def __init__(self):
        # Instancia caminhos de dados
        self.source_data_path = JSON_DATA_PATH
        self.destination_path = PARQUET_DATA_PATH
    
        # Verifica se PARQUET_DATA_PATH existe e cria, caso não exista
        if not os.path.isdir(PARQUET_DATA_PATH):
            os.makedirs(PARQUET_DATA_PATH)

        # Inicia sessão Spark
        self.spark = SparkSession.builder \
            .appName("RuneScapeStream") \
            .getOrCreate()

        self.source_data = self.read_source_data()
    
    def read_source_data(self):
        return self.spark.read.json(self.source_data_path)
    
    def filter_data(self, df, column: str, value: str):
        return df.where(col(column) == value)

    def group_by_count(self, df, column: str):
        return df.groupBy(column).count()

    def convert_to_parquet(self, df):
        df.write.parquet(self.destination_path, mode="overwrite")