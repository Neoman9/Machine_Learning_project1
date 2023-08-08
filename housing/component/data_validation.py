from housing.exception import HousingException
from housing.logger import logging
from housing.entity.config_entity import DataValidationConfig
from housing.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
import os
import sys
import pandas as pd
import json
import yaml 


from evidently.report import Report
from evidently.metric_preset import DataDriftPreset


class DataValidation:

    def __init__(self, data_validation_config: DataValidationConfig,data_ingestion_artifact: DataIngestionArtifact):
        try:
            logging.info(f"{'>>'*30}Data Validation Log Started {'<<'*30} \n\n")
            self.data_validation_config =data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact

        except Exception as e:
            raise HousingException(sys,e) from e
        
    def get_train_and_test_df(self):
        try:
            train_df= pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df= pd.read_csv(self.data_ingestion_artifact.test_file_path)
            return train_df,test_df
        except Exception as e:
            raise HousingException(sys,e) from e 
        
    def is_train_test_file_exists (self)->bool:
        try:
            logging.info("checking to see if train and test file is available")
            is_train_file_exist =False 
            is_test_file_exist = False 

            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path= self.data_ingestion_artifact.test_file_path

            is_train_file_exist=os.path.exists(train_file_path)
            is_test_file_exist= os.path.exists(test_file_path)

            is_available = is_train_file_exist and is_test_file_exist

            logging.info(f"is train and test file exists? -> {is_available}")

            if not is_available:
                training_file= self.data_ingestion_artifact.train_file_path
                testing_file= self.data_ingestion_artifact.test_file_path
                message= f"Training file:{training_file} or Testing file :{testing_file} is not present"

                raise Exception(message)
            
            return is_available



        except Exception as e:
            raise HousingException(sys, e) from e 
        
    def get_and_save_data_drift_report(self):
        try:
            data_drift_report_1 = Report(metrics= [DataDriftPreset(num_stattest='ks', cat_stattest='psi', num_stattest_threshold=0.2, cat_stattest_threshold=0.)])

            train_df, test_df =self.get_train_and_test_df()

    

            data_drift_report_1.run(reference_data=train_df, current_data=test_df)

            report= json.loads(data_drift_report_1.json())

            report_file_path = self.data_validation_config.report_file_path
            report_dir =os.path.dirname(report_file_path)

            os.makedirs(report_dir,exist_ok=True)

            with open(report_file_path,"w") as report_file:
                json.dump(report,report_file, indent =6)

            return report
        except Exception as e :
            raise HousingException(sys,e ) from e 
    
    def save_data_drift_report_page(self):
        try:
            data_drift_report = Report(metrics=[DataDriftPreset(num_stattest='ks', cat_stattest='psi', num_stattest_threshold=0.2, cat_stattest_threshold=0.)])
            train_df,test_df= self.get_train_and_test_df()
            
            data_drift_report.run(reference_data=train_df,current_data=test_df)

            report_page_file_path= self.data_validation_config.report_page_file_path
            report_page_dir= os.path.dirname(report_page_file_path)

            os.makedirs(report_page_dir,exist_ok=True)
            data_drift_report.save_html(report_page_file_path)

        except Exception as e:
            raise HousingException(e,sys) from e 
        
    def is_data_drift_found (self)->bool:
        try:
            report= self.get_and_save_data_drift_report()
            self.save_data_drift_report_page()
            return True
        except Exception as e:
            raise HousingException(sys,e) from e 
        
    def validate_dataset_schema(self)->bool:
        try:
            validation_status = False
            
             #1. Number of Column

            # Load the YAML data from the schema file
            with open(self.data_validation_config.schema_file_path, 'r') as file:
                schema_data = yaml.safe_load(file)

            # Access the 'numerical_columns' attribute from the schema data
            expected_numerical_columns = schema_data['numerical_columns']

            with open(self.data_validation_config.schema_file_path, 'r') as file:
                schema_data = yaml.safe_load(file)

            #access the cat column 
            
            expected_categorical_columns = schema_data['categorical_columns']
            expected_target_column = schema_data['target_column']

            train_df, test_df = self.get_train_and_test_df()
            # Check if the number of columns in train_df matches the expected columns
            if len(train_df.columns) != len(expected_numerical_columns) + len(expected_categorical_columns) + 1:
                print("Error: Number of columns in the training dataset does not match the expected schema.")
                return False
            
             # Check if the number of columns in test_df matches the expected columns
            if len(test_df.columns) != len(expected_numerical_columns) + len(expected_categorical_columns) + 1:
                print("Error: Number of columns in the testing dataset does not match the expected schema.")
                return False 

            # 2. Check the value of ocean proximity
            with open(self.data_validation_config.schema_file_path, 'r') as file:
                schema_data = yaml.safe_load(file)
            expected_ocean_proximity_values = schema_data.get('domain_value', {}).get('ocean_proximity', [])
            ocean_proximity_values_train = train_df['ocean_proximity'].unique()
            ocean_proximity_values_test = test_df['ocean_proximity'].unique()

            if not all(val in expected_ocean_proximity_values for val in ocean_proximity_values_train):
                print("Error: Invalid ocean proximity values in the training dataset.")
                return False
            
            if not all(val in expected_ocean_proximity_values for val in ocean_proximity_values_test):
                print("Error: Invalid ocean proximity values in the testing dataset.")
                return False
            
            # 3. Check column names
            expected_columns = expected_numerical_columns + expected_categorical_columns + expected_target_column
            expected_columns.sort()

            train_columns = list(train_df.columns)
            train_columns.sort()

            test_columns = list(test_df.columns)
            test_columns.sort()

            if train_columns != expected_columns or test_columns != expected_columns:
                print("Error: Column names do not match the expected schema.")
                return False
            
             # If all validation checks pass, set validation_status to True




            validation_status = True
            return validation_status 
        except Exception as e:
            raise HousingException(e,sys) from e
        
    def initiate_data_validation(self)-> DataValidationArtifact:
        try:
            self.is_train_test_file_exists()
            self.validate_dataset_schema()
            self.is_data_drift_found()

            data_validation_artifact = DataValidationArtifact(schema_file_path=self.data_validation_config.schema_file_path, report_file_path=self.data_validation_config.report_file_path,
                                                          report_page_file_path=self.data_validation_config.report_page_file_path,
                                                          is_validated=True,message="Data validation performed successfully")
        
            logging.info(f"Data Validation Artifact :{data_validation_artifact}")
            return data_validation_artifact
        
        except Exception as e:
            raise HousingException(e,sys) from e
        
    def   __del__(self):
        logging.info(f"{'>>'*30}Data Validation log Completed.{'<<'*30} \n\n")

        
    