import os
import sys
from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass, field

import findspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Transformer, Pipeline
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.param.shared import HasInputCols, HasOutputCols



@dataclass
class ConstantsConfig:
    X: list = field(default_factory=lambda: ["mes", "color", "marca_general",
                                                                 "calle1","calle2", "calle3",
                                                                 "colonia", "alcaldia"])
    
    X_freqs: list = field(default_factory=lambda: ["mes_freq", "color_freq", "marca_general_freq",
                                                                 "calle1_freq","calle2_freq", "calle3_freq",
                                                                 "colonia_freq", "alcaldia_freq"])
    y: str= 'categoria'



class FrequencyEncoder_(Transformer, HasInputCols, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self, inputCols=None, outputCols=None):
        super(FrequencyEncoder_, self).__init__()
        self.setParams(inputCols=inputCols, outputCols=outputCols)
        self.freq_cols = {}

    def setParams(self, inputCols=None, outputCols=None):
        return self._set(inputCols=inputCols, outputCols=outputCols)

    def _fit(self, df: DataFrame):
        """
        Ajusta el encoder calculando las frecuencias para las columnas categóricas.
        """
        inputCols = self.getInputCols()
        for col in inputCols:
            window = Window.partitionBy(col)
            freq_col = F.count("*").over(window) / F.count("*").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
            self.freq_cols[col] = freq_col

        return self

    def _transform(self, df: DataFrame):
        """
        Aplica la codificación de frecuencia al conjunto de datos.
        """
        inputCols = self.getInputCols()
        outputCols = self.getOutputCols()


        for input_col, output_col in zip(inputCols, outputCols):
            freq_col = self.freq_cols[input_col]
            df = df.withColumn(output_col, freq_col)

        return df

    def transform(self, df: DataFrame):
        """
        Llamada pública para transformar el DataFrame en un pipeline.
        """
        return self._transform(df)

    def fit(self, df: DataFrame):
        """
        Ajusta y retorna el transformador, necesario en pipelines.
        """
        return self._fit(df)
    

class FrequencyEncoder(Transformer, HasInputCols, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self, inputCols=None, outputCols=None):
        super(FrequencyEncoder, self).__init__()
        self.setParams(inputCols=inputCols, outputCols=outputCols)
        self.freq_cols = {}

    def setParams(self, inputCols=None, outputCols=None):
        return self._set(inputCols=inputCols, outputCols=outputCols)

    def _fit(self, df: DataFrame):
        """
        Ajusta el encoder calculando las frecuencias para las columnas categóricas.
        """
        inputCols = self.getInputCols()
        outputCols = self.getOutputCols()

        try:
            total_count = df.count()
            for col, output in zip(inputCols, outputCols):
                freq_col = df.select(col)\
                             .groupBy(col)\
                             .count()\
                             .select(col,
                                     (F.col('count') / total_count).alias(output))
                            
                self.freq_cols[col] = freq_col
        
        except Exception as e:
            raise CustomException(e,sys)

        return self

    def _transform(self, df: DataFrame):
        """
        Aplica la codificación de frecuencia al conjunto de datos.
        """
        inputCols = self.getInputCols()
        

        for input_col in inputCols:
            df2 = self.freq_cols[input_col]

            df = df.join(df2, input_col, 'inner')

        return df

    def transform(self, df: DataFrame):
        """
        Llamada pública para transformar el DataFrame en un pipeline.
        """
        return self._transform(df)

    def fit(self, df: DataFrame):
        """
        Ajusta y retorna el transformador, necesario en pipelines.
        """
        return self._fit(df)
    
    

@dataclass
class DataPreprocessingConfig:
    train_data_path: str=os.path.join('artifacts', 'stage02', "train.parquet")
    test_data_path: str=os.path.join('artifacts', 'stage02', "test.parquet")
    train_preprocessing_path: str = os.path.join('artifacts', "stage03", "train_preprocessing.parquet")
    test_preprocessing_path: str = os.path.join('artifacts', "stage03", "test_preprocessing.parquet")


class DataPreprocessing:
    def __init__(self):
        self.data_preprocessing_config = DataPreprocessingConfig()
        self.constantsconfig = ConstantsConfig()
        self.spark_init = SparkSession.builder.getOrCreate()

        
    def preprocessing(self, df=None):

        try:
            logging.info(f"-----------------------Initializated data preprocessing--------------------------------------")

            findspark.init()

            train = self.spark_init.read.parquet(self.data_preprocessing_config.train_data_path)\
                                   .select(self.constantsconfig.X+['anio_infraccion', 'dia']+['categoria'])
            test = self.spark_init.read.parquet(self.data_preprocessing_config.test_data_path)\
                                  .select(self.constantsconfig.X+['anio_infraccion', 'dia']+['categoria'])

            #StandardScaler(withMean=True, withStd=True, inputCol="features", outputCol="StandardFeatures")

            frecuency_encoder = FrequencyEncoder(inputCols=self.constantsconfig.X, outputCols=self.constantsconfig.X_freqs)

            if df == None:
                
                pipeline = Pipeline(stages=[
                    StringIndexer(inputCol=self.constantsconfig.y, outputCol="label"),
                    VectorAssembler(inputCols=self.constantsconfig.X_freqs+["dia"], outputCol="StandardFeatures")])

                #TRAIN
                train_frecuency_encoder = frecuency_encoder.fit(train).transform(train)  ##esto son datos

                train_pipeline = pipeline.fit(train_frecuency_encoder).transform(train_frecuency_encoder)\
                                        .select("label", "StandardFeatures")
                        
                train_pipeline.write.mode("overwrite").parquet(self.data_preprocessing_config.train_preprocessing_path)

                #TEST
                test_frecuency_encoder = frecuency_encoder.fit(train).transform(test)  ##estos son datos



                test_pipeline = pipeline.fit(train_frecuency_encoder).transform(test_frecuency_encoder)\
                                        .select("label", "StandardFeatures")
                
                
                test_pipeline.write.mode("overwrite").parquet(self.data_preprocessing_config.test_preprocessing_path)

            else:

                train = train.drop("categoria")
                
                pipeline = Pipeline(stages=[
                    VectorAssembler(inputCols=self.constantsconfig.X_freqs+["dia"], outputCol="StandardFeatures")])
                
                #TRAIN
                train_frecuency_encoder = frecuency_encoder.fit(train).transform(train)

                #DF
                df_frecuency_encoder = frecuency_encoder.fit(train).transform(df)

                df_pipeline = pipeline.fit(train_frecuency_encoder).transform(df_frecuency_encoder)\
                                      .select("StandardFeatures")
                
                return df_pipeline
            

            logging.info(f"-----------------------Finished data preprocessing--------------------------------------")
        
        except Exception as e:
            raise CustomException(e,sys)
        


if __name__=="__main__":
    data_preprocessing = DataPreprocessing()
    data_preprocessing.preprocessing()
