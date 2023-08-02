from collections import namedtuple



DataIngetionConfig= namedtuple("DataIngestionConfig",
                               ["dataset_download_url","tgz_download_dir","raw_data_dir","ingested_train_dir","ingested_test_dir"])





TrainingPipelineConfig = namedtuple("TrainingPipelineConfig", ["artifact_dir"])