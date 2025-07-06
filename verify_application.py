"""
Script to verify that the Spark + DBT + Delta Lake application is working correctly.

This script:
1. Creates sample data
2. Runs the pipeline
3. Verifies the output

Usage:
    python verify_application.py
"""

import os
import sys
import time
import shutil
from src.main import create_sample_data
from src.pipeline import Pipeline
from src.utils.logger import setup_logger

# Set up logger
logger = setup_logger(__name__)

def main():
    """Run the verification process."""
    logger.info("Starting application verification")
    
    # Set environment variables
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Create sample data
    sample_path = os.path.join(os.getcwd(), "data", "input", "verification_sample.csv")
    create_sample_data(sample_path)
    
    # Verify sample data exists
    if not os.path.exists(sample_path):
        logger.error(f"Failed to create sample data at {sample_path}")
        return False
    
    logger.info("Sample data created successfully")
    
    # Create and run the pipeline
    try:
        # Create pipeline
        pipeline = Pipeline(app_name="VerificationPipeline")
        
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
        start_time = time.time()
        result = pipeline.run_pipeline(
            source_path=sample_path,
            source_type="csv",
            transformations=[add_salary_category]
        )
        execution_time = time.time() - start_time
        
        # Verify pipeline completed successfully
        if result:
            logger.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
            logger.info("Application verification PASSED")
            
            # Clean up
            if os.path.exists(sample_path):
                os.remove(sample_path)
                logger.info(f"Removed verification sample data at {sample_path}")
            
            return True
        else:
            logger.error("Pipeline failed to complete")
            logger.info("Application verification FAILED")
            return False
    
    except Exception as e:
        logger.error(f"Error during verification: {str(e)}")
        logger.info("Application verification FAILED")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)