from housing.component.data_ingestion import DataIngestion
from housing.exception import HousingException
from housing.logger import logging
import sys,os
from housing.config.configuration import configuration
from collections import namedtuple 
from threading import Thread
from housing.entity.experiment import Experiment
from housing.config.configuration import configuration 
from housing.constants import EXPERIMENT_DIR_NAME,EXPERIMENT_FILE_NAME
from housing.entity.artifact_entity import DataIngestionArtifact

Experiment = namedtuple("Experiment",["experiment_id", "initialization_timestamp","artifact_time_stamp","running_status",
                                      "start_time","stop_time","execution_time","message","experiment_file_path","accuracy","is_model_accepted"])




class Pipeline(Thread):
    experiment: Experiment= Experiment(*([None] *11))
    experiment_file_path = None 

    def __init__(self, config: configuration)-> None:
        try:
            os.makedirs(config.training_pipeline_config.artifact_dir, exist_ok=True)
            Pipeline.experiment_file_path=os.path.join(config.training_pipeline_config.artifact_dir,EXPERIMENT_DIR_NAME,EXPERIMENT_FILE_NAME)
            super().__init__(daemon=False, name= "pipeline")
            self.config = config 

        except Exception as e :
            raise HousingException(sys,e) from e 
        
    def start_data_ingestion(self)-> DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        
        except Exception as e :
            raise HousingException(e,sys) from e 
        
        