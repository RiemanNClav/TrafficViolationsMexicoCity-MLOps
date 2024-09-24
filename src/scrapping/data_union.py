import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd

from dataclasses import dataclass, field

from pyspark.sql import SparkSession, DataFrame

from src.utils import categoria_mas_parecida

import pyspark.sql.types as T
import pyspark.sql.functions as F




@dataclass
class DataUnionConfig:
    raw_data_path: str=os.path.join('notebook','data',"data.parquet")

    columns: list = field(default_factory=lambda: ['id_infraccion', 'fecha_infraccion', 'ao_infraccion', 'mes', 'categoria', 
                    'articulo', 'fraccion', 'inciso', 'parrafo', 'placa', 
                    'Color', 'marca_general', 'marca', 'submarca', 'en_la_calle', 
                    'entre_calle', 'y_calle', 'colonia', 'alcaldia', 'longitud', 'latitud'])

class DataUnion:
    def __init__(self):
        self.ingestion_config=DataUnionConfig()
        self.spark_init =  SparkSession.builder.getOrCreate()

    def initiate_data_union(self):
        logging.info("Initializated data union")
        try:

            file_path = os.path.join('notebook','data',"corruptos.txt")
            os.remove(file_path)

            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path),exist_ok=True)

            directorio = 'notebook/data/'
            palabra_clave = 'infracciones_infracciones_transito'
            dataframes = []
            str = ''
            for archivo in os.listdir(directorio):
                if archivo.endswith('.csv') and palabra_clave in archivo:
                    path = os.path.join(directorio, archivo)
                    df = self.spark_init.read.csv(path, header=True, inferSchema=True, encoding="ISO-8859-1")

                    try:
                        if df.columns == self.ingestion_config.columns:

                            df = df.select(self.ingestion_config.columns)

                        else:
                            new_names = {col: categoria_mas_parecida(col, self.ingestion_config.columns) for col in df.columns}
                            for old_name, new_name in new_names.items():
                                df = df.withColumnRenamed(old_name, new_name)
        
                            df = df.select(self.ingestion_config.columns)
                        
                        df = df.select(F.col("id_infraccion").cast(T.StringType()).alias("id_infraccion"),
                                       "fecha_infraccion",
                                        F.col("ao_infraccion").cast(T.IntegerType()).alias("anio_infraccion"),
                                        F.col("mes").cast(T.StringType()).alias("mes"),
                                        F.col("categoria").cast(T.StringType()).alias("categoria"),
                                        F.col("articulo").cast(T.StringType()).alias("articulo"),
                                        F.col("fraccion").cast(T.StringType()).alias("fraccion"),
                                        F.col("inciso").cast(T.StringType()).alias("inciso"),
                                        F.col("parrafo").cast(T.StringType()).alias("parrafo"),
                                        F.col("placa").cast(T.StringType()).alias("placa"),
                                        F.col("Color").cast(T.StringType()).alias("Color"),
                                        F.col("marca_general").cast(T.StringType()).alias("marca_general"),
                                        F.col("submarca").cast(T.StringType()).alias("submarca"),
                                        F.col("en_la_calle").cast(T.StringType()).alias("calle1"),
                                        F.col("entre_calle").cast(T.StringType()).alias("calle2"),
                                        F.col("y_calle").cast(T.StringType()).alias("calle3"),
                                        F.col("colonia").cast(T.StringType()).alias("colonia"),
                                        F.col("alcaldia").cast(T.StringType()).alias("alcaldia"),
                                        F.col("longitud").cast(T.IntegerType()).alias("longitud"),
                                        F.col("latitud").cast(T.IntegerType()).alias("latitud"))

                        df.write.mode("append").parquet(self.ingestion_config.raw_data_path)
                        os.remove(path)
                        
                    except:
                        str += archivo + '\n'

            with open(file_path, 'w') as file:
                file.write(str)

            logging.info("Finished data union")

            return(self.ingestion_config.raw_data_path)
        
        except Exception as e:
            print(archivo)
            print(df.columns)
            raise CustomException(e,sys)
        
if __name__=="__main__":
    obj=DataUnion()
    obj.initiate_data_union()