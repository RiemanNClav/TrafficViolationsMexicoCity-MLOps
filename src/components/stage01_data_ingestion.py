import os
import sys
from src.exception import CustomException
from src.logger import logging

import pandas as pd

import findspark
from pyspark.sql import SparkSession

from dataclasses import dataclass


@dataclass
class DataIngestionConfig:
    train_data_path: str=os.path.join('artifacts', 'stage01', "raw_train.parquet")
    test_data_path: str=os.path.join('artifacts', 'stage01', "raw_test.parquet")
    raw_data_path: str=os.path.join('notebook', 'data', "data.parquet")

class DataIngestion:
    def __init__(self):

        self.ingestion_config=DataIngestionConfig()
        self.spark_init = SparkSession.builder.getOrCreate()


    def initiate_data_ingestion(self):
        logging.info("Initializated data ingestion")
        try:
            findspark.init()
            logging.info('Read the dataset as pyspark dataframe')

            df = self.spark_init.read.parquet(self.ingestion_config.raw_data_path)
            #df = self.spark_init.read.csv('notebook/data/data.csv', header=True, inferSchema=True, encoding="ISO-8859-1")
            
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path),exist_ok=True)
            logging.info("Train test split initiated")
            train_set, test_set = df.randomSplit([0.8, 0.2])

            train_set.write.mode("overwrite").parquet(self.ingestion_config.train_data_path)
            test_set.write.mode("overwrite").parquet(self.ingestion_config.test_data_path)

            logging.info("Finished data ingestion")

            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path

            )
        except Exception as e:
            raise CustomException(e,sys)
        
if __name__=="__main__":
    obj=DataIngestion()
    obj.initiate_data_ingestion()