"""
Script to run the Spark + DBT + Delta Lake pipeline with sample data.
"""

import os
import sys
from src.pipeline import Pipeline
from src.utils.logger import setup_logger

# Set up logger
logger = setup_logger(__name__)

def main():
    """
    Main function to run the pipeline with sample data.
    """
    # Set environment variables
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Define input path
    input_path = os.path.join(os.getcwd(), "data", "input", "sample.csv")
    
    # Validate input path
    if not os.path.exists(input_path):
        logger.error(f"Sample data not found at {input_path}")
        logger.info("Please run 'python -m src.main --create-sample' to create sample data")
        return
    
    # Create and run the pipeline
    try:
        # Create pipeline
        logger.info("Creating pipeline")
        pipeline = Pipeline(app_name="SparkDeltaDBT-Example")
        
        # Define custom transformations
        def add_salary_category(df):
            """Add a salary category column based on salary."""
            from pyspark.sql import functions as F
            return df.withColumn(
                "salary_category",
                F.when(F.col("salary") < 70000, "Low")
                .when(F.col("salary") < 85000, "Medium")
                .otherwise("High")
            )
        
        # Run the pipeline
        logger.info(f"Running pipeline with sample data: {input_path}")
        pipeline.run_pipeline(
            source_path=input_path,
            source_type="csv",
            transformations=[add_salary_category]
        )
        
        logger.info("Pipeline completed successfully")
        logger.info("Check the 'data/raw' and 'data/processed' directories for output")
    
    except Exception as e:
        logger.error(f"Error running pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()