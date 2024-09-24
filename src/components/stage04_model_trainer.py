
#de ley
import os
import sys
from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass


#basicos de pyspark
import findspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

#modelos
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.classification import RandomForestClassifier

#evaluacion
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


@dataclass
class ModelTrainerConfig:
    train_data_path: str=os.path.join('artifacts', 'stage03', "test_preprocessing.parquet")
    test_data_path: str=os.path.join('artifacts', 'stage03', "train_preprocessing.parquet")
    model_path: str = os.path.join('artifacts', "model.parquet")


class ModelTrainer:
    def __init__(self):
        self.spark_init = SparkSession.builder.getOrCreate()
        self.model_trainer_config = ModelTrainerConfig()

    def modeltrainer(self, df=None):

        try:
            logging.info(f"-----------------------Initializated model trainer--------------------------------------")

            findspark.init()

            # MULTINOMIAL RANDOM FOREST

            train = self.spark_init.read.parquet(self.model_trainer_config.train_data_path)
            test = self.spark_init.read.parquet(self.model_trainer_config.test_data_path)

            rf = RandomForestClassifier(featuresCol="StandardFeatures", labelCol="label", numTrees=15)

            rf_model = rf.fit(train)

            if df == None:

                    print('MULTINOMIAL RANDOM FOREST\n')
                    

                    evaluator_rf = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

                    #TRAIN
                    rf_predictions = rf_model.transform(train)
                    rf_predictions = rf_predictions.select("StandardFeatures", "label", "prediction")
                    rf_predictions = rf_predictions.withColumn("prediction", F.col("prediction").cast("double"))\
                                                   .withColumn("label", F.col("label").cast("double"))
                    
                    accuracy_train = evaluator_rf.evaluate(rf_predictions)
                    print('train_accuracy', accuracy_train)

                    #TEST
                    rf_predictions = rf_model.transform(test)
                    rf_predictions = rf_predictions.select("StandardFeatures", "label", "prediction")
                    rf_predictions = rf_predictions.withColumn("prediction", F.col("prediction").cast("double"))\
                                                   .withColumn("label", F.col("label").cast("double"))
                    
                    accuracy_test = evaluator_rf.evaluate(rf_predictions)
                    print('test_accuracy', accuracy_test)

                    return ('Multinomial Random Forest', accuracy_train, accuracy_test)

            else:
                 
                rf_predictions = rf_model.transform(df)
                rf_predictions = rf_predictions.select("StandardFeatures", "prediction")\
                                               .withColumn("prediction", F.col("prediction").cast("double"))\
                                               .select('prediction')
                return rf_predictions
            
            logging.info(f"-----------------------Initializated model trainer--------------------------------------")
        
        except Exception as e:
            raise CustomException(e,sys)
        


if __name__=="__main__":
    model_trainer = ModelTrainer()
    model_trainer.modeltrainer()
