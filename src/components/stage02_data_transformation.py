# de ley
import sys
from dataclasses import dataclass, field


#modulos
from src.exception import CustomException
from src.logger import logging
import os
from src.utils import detect_nulls

#spark
import findspark

from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.param.shared import HasInputCols, HasOutputCols


@dataclass
class ConstantsConfig:
    variables_categoricas: list = field(default_factory=lambda: ["mes", "fraccion", "inciso", "parrafo", 'categoria',
                                                                 "placa", "color", "marca_general", "submarca", "calle1",
                                                                 "calle2", "calle3", "colonia", "alcaldia", "articulo"])
    variables_numericas: list = field(default_factory=lambda: ["anio_infraccion", "longitud", "latitud"])


class DataCleaning(object):
    def __init__(self):
        self.constantsconfig = ConstantsConfig()

    def initiate_data_cleaning(self, df: DataFrame):
        try:
            df = df.select(F.col("id_infraccion").cast(T.StringType()).alias("id_infraccion"),
                           "fecha_infraccion",
                            F.col("anio_infraccion").cast(T.IntegerType()).alias("anio_infraccion"),
                            F.col("mes").cast(T.StringType()).alias("mes"),
                            F.col("categoria").cast(T.StringType()).alias("categoria"),
                            F.col("articulo").cast(T.StringType()).alias("articulo"),
                            F.col("fraccion").cast(T.StringType()).alias("fraccion"),
                            F.col("inciso").cast(T.StringType()).alias("inciso"),
                            F.col("parrafo").cast(T.StringType()).alias("parrafo"),
                            F.col("placa").cast(T.StringType()).alias("placa"),
                            F.col("Color").cast(T.StringType()).alias("color"),
                            F.col("marca_general").cast(T.StringType()).alias("marca_general"),
                            F.col("submarca").cast(T.StringType()).alias("submarca"),
                            F.col("calle1").cast(T.StringType()).alias("calle1"),
                            F.col("calle2").cast(T.StringType()).alias("calle2"),
                            F.col("calle3").cast(T.StringType()).alias("calle3"),
                            F.col("colonia").cast(T.StringType()).alias("colonia"),
                            F.col("alcaldia").cast(T.StringType()).alias("alcaldia"),
                            F.col("longitud").cast(T.IntegerType()).alias("longitud"),
                            F.col("latitud").cast(T.IntegerType()).alias("latitud"))\
                    .drop(*['latitud', 'longitud'])\
                    .withColumn("fecha_infraccion", F.to_date(F.col("fecha_infraccion"), "dd/MM/yyyy"))\
                    .select('*',
                            F.dayofmonth(F.col("fecha_infraccion")).cast(T.IntegerType()).alias("dia"),
                            F.month(F.col("fecha_infraccion")).cast(T.IntegerType()).alias("mes_numero"))
            
            for col in self.constantsconfig.variables_categoricas:
                df = df.withColumn(col, F.translate(F.regexp_replace(F.upper(F.col(col)), "[.,!¡\?¿]", ""), 'ÁÉÍÓÚ', 'AEIOU'))

            return df
        
        except Exception as e:
            raise CustomException(e,sys)
        
    
class Categoricalmputer(Transformer, HasInputCols, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self):
        super(Categoricalmputer, self).__init__()
        self.setParams(inputCols=None, outputCols=None)
        self.constantsconfig = ConstantsConfig()
        self.mode_dict = {}

    def setParams(self, inputCols=None, outputCols=None):
        return self._set(inputCols=inputCols, outputCols=outputCols)
    
    def detecting_nulls(self, df: DataFrame):
        categoricas_imputation = detect_nulls(df, 0.7, self.constantsconfig.variables_categoricas)
        return categoricas_imputation
        
    def _fit(self, df: DataFrame):
        """
        Fit the imputer to the DataFrame to learn the mode of categorical columns.
        """
        categoricas_imputation = self.detecting_nulls(df)

        for column in categoricas_imputation:
            mode_value = df.filter(F.col(column).isNotNull()) \
                              .groupBy(column) \
                              .count() \
                              .orderBy(F.desc("count")) \
                              .first()[0]
            self.mode_dict[column] = mode_value

        return self
    
    def _transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the DataFrame by imputing missing values with the mode.
        """
        for column, mode in self.mode_dict.items():
            df = df.fillna({column: mode})
        return df
    
    def transform(self, df: DataFrame):
        """
        Ajusta y transforma el conjunto de datos de entrenamiento.
        """
        return self._transform(df)
    
    def fit(self, df: DataFrame):

        return self._fit(df)
    

@dataclass
class DataTransformationConfig:
        raw_train_path: str=os.path.join('artifacts', 'stage01',"raw_test.parquet")
        raw_test_path: str=os.path.join('artifacts', 'stage01', "raw_train.parquet")
        train_data_path: str=os.path.join('artifacts', 'stage02', "train.parquet")
        test_data_path: str=os.path.join('artifacts', 'stage02', "test.parquet")


class DataTransformation:
    def __init__(self):

        self.transformation_config=DataTransformationConfig()
        self.spark_init = SparkSession.builder.getOrCreate()


    def initiate_data_transformation(self):
        logging.info("----------------Initializated data transformation--------------------------")
        try:
            findspark.init()
            raw_train = self.spark_init.read.parquet(self.transformation_config.raw_train_path)
            raw_test = self.spark_init.read.parquet(self.transformation_config.raw_test_path)
            

            data_cleaning = DataCleaning()
            train = data_cleaning.initiate_data_cleaning(raw_train)
            test = data_cleaning.initiate_data_cleaning(raw_test)

            frequency_encoder = Categoricalmputer()
            train_encoded = frequency_encoder.fit(train).transform(train)
            test_encoded = frequency_encoder.fit(train).transform(test)

            train_encoded.write.mode("overwrite").parquet(self.transformation_config.train_data_path)
            test_encoded.write.mode("overwrite").parquet(self.transformation_config.test_data_path)

            logging.info("-----------------Finished data transformation--------------------------")

            return(
                self.transformation_config.train_data_path,
                self.transformation_config.test_data_path

            )
        except Exception as e:
            raise CustomException(e,sys)


if __name__=="__main__":

    data_transformation = DataTransformation()
    data_transformation.initiate_data_transformation()
    