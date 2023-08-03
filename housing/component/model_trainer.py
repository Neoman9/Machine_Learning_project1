from housing.exception import HousingException
from housing.logger import logging
from housing.entity.config_entity import ModelTrainerConfig
from housing.entity.artifact_entity import ModelTrainerArtifact,DataIngestionArtifact,DataTransformationArtifact
from housing.util.util import load_numpy_array_data,load_data

import sys,os


class  HousingEstimatorModel:
    def __init__(self, preprocessing_object, trained_model_object):
        
        """TrainedModel constructor
        preprocessing_object: preprocessing_object
        trained_model_object: trained_model_object"""
        self.preprocessing_object = preprocessing_object
        self.trained_model_object= trained_model_object

    def predict(self,X):
        """ function accepts raw inputs and then transformed raw input using preprocessing_object
        which gurantees that the inputs are in the same format as the training data
        At last it perform prediction on transformed features"""
        transformed_feature = self.preprocessing_object.transform(X)
        return self.trained_model_object.predict(transformed_feature)
    
    def __repr__(self):
        return f"{type(self.trained_model_object).__name__}()"
    
    def __str__(self):
        return f"{type(self.trained_model_object).__name__}()"
    
class ModelTrainer:
    def __init__ (self, model_trainer_config: ModelTrainerConfig, data_transformation_artifact: DataTransformationArtifact):

        try:
            logging.info(f"{'>>' *30} Model Trainer log started. {'<<' *30}")
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact
        
        except Exception as e :
            raise HousingException(sys,e ) from e
        
    def initiate_model_trainer(self) -> ModelTrainerArtifact:
        try:
            logging.info(f"loading tansformed training dataset")
            tranformed_train_file_path = self.data_transformation_artifact.transformed_train_file_path
            train_array = load_numpy_array_data(file_path=tranformed_train_file_path)

            logging.info(f" loading tranformed testing data set.")
            transformed_test_file_path = self.data_transformation_artifact.transformed_test_file_path
            test_array =load_numpy_array_data(file_path=transformed_test_file_path)
            

        except Exception as e:
            raise HousingException(sys,e ) from e
        