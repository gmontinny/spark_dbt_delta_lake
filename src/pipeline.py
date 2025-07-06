"""
Module for orchestrating the entire data pipeline.
"""

import os
import time
from datetime import datetime
from src.utils.logger import setup_logger
from src.utils.spark_session import create_spark_session, stop_spark_session
from src.ingestion.data_reader import DataReader
from src.transformation.data_transformer import DataTransformer
from src.utils.dbt_runner import DBTRunner
from config.config import APP_CONFIG

# Set up logger
logger = setup_logger(__name__)

class Pipeline:
    """
    Class for orchestrating the entire data pipeline.
    """
    
    def __init__(self, app_name="SparkDeltaDBT", config=None):
        """
        Initialize the Pipeline.
        
        Args:
            app_name (str): Name of the Spark application
            config (dict, optional): Configuration dictionary
        """
        self.app_name = app_name
        self.config = config or APP_CONFIG
        self.spark = None
        self.reader = None
        self.transformer = None
        self.dbt_runner = None
        
        logger.info(f"Pipeline initialized with app_name={app_name}")
    
    def start(self):
        """
        Start the pipeline by initializing all components.
        
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Starting pipeline")
        
        try:
            # Create Spark session
            self.spark = create_spark_session(app_name=self.app_name)
            
            # Initialize components
            self.reader = DataReader(
                spark=self.spark,
                input_dir=self.config.get('input_directory'),
                output_dir=os.path.join(os.getcwd(), "data", "raw")
            )
            
            self.transformer = DataTransformer(
                spark=self.spark,
                input_dir=os.path.join(os.getcwd(), "data", "raw"),
                output_dir=os.path.join(os.getcwd(), "data", "processed")
            )
            
            self.dbt_runner = DBTRunner(
                project_dir=os.path.join(os.getcwd(), "models"),
                profiles_dir=os.path.join(os.path.expanduser("~"), ".dbt")
            )
            
            logger.info("Pipeline started successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error starting pipeline: {str(e)}")
            self.stop()
            raise
    
    def stop(self):
        """
        Stop the pipeline and clean up resources.
        
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Stopping pipeline")
        
        try:
            # Stop Spark session
            if self.spark:
                stop_spark_session(self.spark)
                self.spark = None
            
            logger.info("Pipeline stopped successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error stopping pipeline: {str(e)}")
            raise
    
    def ingest_data(self, source_path, source_type="csv", options=None):
        """
        Ingest data from a source into the raw data layer.
        
        Args:
            source_path (str): Path to the source data
            source_type (str): Type of source data (csv, json, parquet)
            options (dict, optional): Additional options for reading data
            
        Returns:
            tuple: (DataFrame, output_path)
        """
        logger.info(f"Ingesting data from {source_path}")
        
        try:
            # Ensure pipeline is started
            if not self.spark or not self.reader:
                self.start()
            
            # Read data based on source type
            if source_type.lower() == "csv":
                df = self.reader.read_csv(source_path, options=options)
            elif source_type.lower() == "json":
                df = self.reader.read_json(source_path, options=options)
            elif source_type.lower() == "parquet":
                df = self.reader.read_parquet(source_path, options=options)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Save as raw data
            source_name = os.path.basename(source_path).split(".")[0]
            output_path = self.reader.save_as_raw(df, source_name)
            
            logger.info(f"Successfully ingested data from {source_path} to {output_path}")
            return df, output_path
        
        except Exception as e:
            logger.error(f"Error ingesting data: {str(e)}")
            raise
    
    def transform_data(self, df=None, source_path=None, transformations=None, name=None):
        """
        Transform data from the raw data layer to the processed data layer.
        
        Args:
            df (DataFrame, optional): DataFrame to transform
            source_path (str, optional): Path to the raw data
            transformations (list, optional): List of transformation functions
            name (str, optional): Name of the dataset
            
        Returns:
            tuple: (DataFrame, output_path)
        """
        logger.info("Transforming data")
        
        try:
            # Ensure pipeline is started
            if not self.spark or not self.transformer:
                self.start()
            
            # Get DataFrame if not provided
            if df is None and source_path:
                df = self.spark.read.parquet(source_path)
            elif df is None:
                raise ValueError("Either df or source_path must be provided")
            
            # Clean data
            df = self.transformer.clean_data(df)
            
            # Add metadata columns
            source_name = name or (os.path.basename(source_path).split("_")[0] if source_path else "unknown")
            df = self.transformer.add_metadata_columns(df, source_name=source_name)
            
            # Apply custom transformations
            if transformations:
                df = self.transformer.apply_transformations(df, transformations)
            
            # Save as processed data
            output_path = self.transformer.save_as_processed(df, source_name)
            
            logger.info(f"Successfully transformed data to {output_path}")
            return df, output_path
        
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            raise
    
    def run_dbt_models(self, models=None, exclude=None, vars=None, full_refresh=False):
        """
        Run DBT models on the processed data.
        
        Args:
            models (list, optional): List of models to run
            exclude (list, optional): List of models to exclude
            vars (dict, optional): Variables to pass to DBT
            full_refresh (bool): Whether to do a full refresh
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Running DBT models")
        
        try:
            # Ensure pipeline is started
            if not self.dbt_runner:
                self.start()
            
            # Run DBT models
            success = self.dbt_runner.run_models(
                models=models,
                exclude=exclude,
                vars=vars,
                full_refresh=full_refresh
            )
            
            if success:
                logger.info("Successfully ran DBT models")
            else:
                logger.error("Failed to run DBT models")
            
            return success
        
        except Exception as e:
            logger.error(f"Error running DBT models: {str(e)}")
            return False  # Don't raise, just return False
    
    def run_pipeline(self, source_path, source_type="csv", transformations=None, dbt_models=None):
        """
        Run the entire pipeline from ingestion to DBT models.
        
        Args:
            source_path (str): Path to the source data
            source_type (str): Type of source data (csv, json, parquet)
            transformations (list, optional): List of transformation functions
            dbt_models (list, optional): List of DBT models to run
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Running pipeline for {source_path}")
        
        try:
            # Start the pipeline
            self.start()
            
            # Track execution time
            start_time = time.time()
            
            # Ingest data
            df, raw_path = self.ingest_data(source_path, source_type)
            
            # Transform data
            df, processed_path = self.transform_data(df, transformations=transformations)
            
            # Run DBT models
            dbt_success = self.run_dbt_models(models=dbt_models)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Log success
            logger.info(f"Pipeline completed in {execution_time:.2f} seconds")
            logger.info(f"Raw data: {raw_path}")
            logger.info(f"Processed data: {processed_path}")
            logger.info(f"DBT models: {'Success' if dbt_success else 'Failed'}")
            
            # Stop the pipeline
            self.stop()
            
            return True
        
        except Exception as e:
            logger.error(f"Error running pipeline: {str(e)}")
            self.stop()
            raise