import sys
import os
import pandas as pd
from src.exception import CustomException

from src.components.stage03_data_preprocessing import DataPreprocessing
from src.components.stage04_model_trainer import ModelTrainer

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
import findspark
import pyspark.pandas as ps



class PredictPipeline:
    def __init__(self):
        pass

    def predict(self,features):
        try:
            data_preprocessing = DataPreprocessing()

            model_trainer = ModelTrainer()

            data_scaled=data_preprocessing.preprocessing(features)

            preds = model_trainer.modeltrainer(data_scaled)

            return preds
        
        except Exception as e:
            raise CustomException(e,sys)


class CustomData:
    def __init__(  self,
        mes: str,
        dia: int,
        color: str,
        marca_general: str,
        calle1: str,
        calle2: str,
        calle3: str,
        colonia: str,
        alcaldia: str):


        self.mes = mes
        self.dia = dia
        self.color = color
        self.marca_general = marca_general
        self.calle1 = calle1
        self.calle2 = calle2
        self.calle3 = calle3
        self.colonia = colonia
        self.alcaldia = alcaldia


    def get_data_as_data_frame(self):
        try:
            findspark.init()
            spark = SparkSession.builder.getOrCreate()

            custom_data_input_dict = {
                "mes": [self.mes],
                "color": [self.color],
                "marca_general": [self.marca_general],
                "calle1": [self.calle1],
                "calle2": [self.calle2],
                "calle3": [self.calle3],
                "colonia": [self.colonia],
                "alcaldia": [self.alcaldia],
                'dia': [self.dia]}

            schema_ = T.StructType([
                T.StructField("mes", T.StringType(), True),
                T.StructField("color", T.StringType(), True),
                T.StructField("marca_general", T.StringType(), True),
                T.StructField("calle1", T.StringType(), True),
                T.StructField("calle2", T.StringType(), True),
                T.StructField("calle3", T.StringType(), True),
                T.StructField("colonia", T.StringType(), True),
                T.StructField("alcaldia", T.StringType(), True),
                T.StructField("dia", T.IntegerType(), True)
            ])

            df_pandas = pd.DataFrame(custom_data_input_dict)
            
            df_spark = spark.createDataFrame(df_pandas, schema=schema_)


            return df_spark

        except Exception as e:
            raise CustomException(e, sys)


if __name__=="__main__":
    data = CustomData('FEBRERO',
                      14,
                      'GRIS',
                      'NISSAN',
                      'INSURGENTES NORTE',
                      "CALZADA TICOMAN",
                      "AV MONTEVIDEO",
                      'TEPEYAC INSURGENTES',
                      "GUSTAVO A MADERO")


    pred_df = data.get_data_as_data_frame()

    print(pred_df)

    predict_pipeline=PredictPipeline()

    results=predict_pipeline.predict(pred_df)

    print(results.show(4,False))