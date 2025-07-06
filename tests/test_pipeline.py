"""
Test script to verify the Spark + DBT + Delta Lake pipeline is working correctly.
"""

import os
import sys
import unittest
import shutil
from pyspark.sql import SparkSession
from src.pipeline import Pipeline
from src.utils.spark_session import create_spark_session, stop_spark_session
from src.utils.logger import setup_logger

# Set up logger
logger = setup_logger(__name__)

class TestPipeline(unittest.TestCase):
    """Test case for the Spark + DBT + Delta Lake pipeline."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Set environment variables
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
        # Create sample data directory
        cls.input_dir = os.path.join(os.getcwd(), "data", "input")
        cls.sample_path = os.path.join(cls.input_dir, "test_sample.csv")
        os.makedirs(cls.input_dir, exist_ok=True)
        
        # Create sample data
        with open(cls.sample_path, 'w') as f:
            f.write("id,name,age,city,salary\n")
            f.write("1,John Doe,30,New York,75000\n")
            f.write("2,Jane Smith,25,San Francisco,85000\n")
            f.write("3,Bob Johnson,40,Chicago,65000\n")
            f.write("4,Alice Brown,35,Boston,80000\n")
            f.write("5,Charlie Davis,45,Seattle,90000\n")
        
        logger.info(f"Created test sample data at {cls.sample_path}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Remove test sample file
        if os.path.exists(cls.sample_path):
            os.remove(cls.sample_path)
            logger.info(f"Removed test sample data at {cls.sample_path}")
    
    def test_spark_session_creation(self):
        """Test that a Spark session can be created successfully."""
        try:
            # Create Spark session
            spark = create_spark_session(app_name="TestSparkSession")
            
            # Verify Spark session is created
            self.assertIsNotNone(spark)
            self.assertIsInstance(spark, SparkSession)
            self.assertEqual(spark.conf.get("spark.app.name"), "TestSparkSession")
            
            # Verify Delta Lake is configured
            self.assertEqual(
                spark.conf.get("spark.sql.extensions"),
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            
            # Stop Spark session
            stop_spark_session(spark)
            logger.info("Spark session test passed")
        except Exception as e:
            self.fail(f"Spark session creation failed: {str(e)}")
    
    def test_pipeline_execution(self):
        """Test that the pipeline can be executed successfully."""
        try:
            # Create pipeline
            pipeline = Pipeline(app_name="TestPipeline")
            
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
            df, raw_path = pipeline.ingest_data(
                source_path=self.sample_path,
                source_type="csv"
            )
            
            # Verify data was ingested
            self.assertIsNotNone(df)
            self.assertEqual(df.count(), 5)  # 5 rows in sample data
            
            # Transform data
            df_transformed, processed_path = pipeline.transform_data(
                df=df,
                transformations=[add_salary_category]
            )
            
            # Verify transformation was applied
            self.assertIsNotNone(df_transformed)
            self.assertTrue("salary_category" in df_transformed.columns)
            
            # Verify data in each category
            low_count = df_transformed.filter("salary_category = 'Low'").count()
            medium_count = df_transformed.filter("salary_category = 'Medium'").count()
            high_count = df_transformed.filter("salary_category = 'High'").count()
            
            self.assertEqual(low_count, 1)  # One person with salary < 70000
            self.assertEqual(medium_count, 2)  # Two people with salary between 70000 and 85000
            self.assertEqual(high_count, 2)  # Two people with salary > 85000
            
            # Stop the pipeline
            pipeline.stop()
            logger.info("Pipeline execution test passed")
        except Exception as e:
            self.fail(f"Pipeline execution failed: {str(e)}")
    
    def test_full_pipeline(self):
        """Test the full pipeline execution."""
        try:
            # Create pipeline
            pipeline = Pipeline(app_name="TestFullPipeline")
            
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
            
            # Run the full pipeline
            result = pipeline.run_pipeline(
                source_path=self.sample_path,
                source_type="csv",
                transformations=[add_salary_category]
            )
            
            # Verify pipeline completed successfully
            self.assertTrue(result)
            logger.info("Full pipeline test passed")
        except Exception as e:
            self.fail(f"Full pipeline execution failed: {str(e)}")

if __name__ == "__main__":
    unittest.main()