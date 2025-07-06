"""
Utility module for creating and managing Spark sessions with Delta Lake support.
"""

import os
import sys
from pyspark.sql import SparkSession
from config.config import ENV_VARS, APP_CONFIG
from src.utils.logger import setup_logger

# Set up logger
logger = setup_logger(__name__)

def setup_environment():
    """
    Set up environment variables required for Spark and Delta Lake.
    """
    logger.info("Setting up environment variables for Spark")

    # Set environment variables
    for key, value in ENV_VARS.items():
        if value:
            os.environ[key] = value
            logger.debug(f"Set environment variable {key}={value}")

    # Add Spark and Hadoop to system path if needed
    if ENV_VARS.get('SPARK_HOME') and ENV_VARS.get('SPARK_HOME') not in sys.path:
        sys.path.append(ENV_VARS.get('SPARK_HOME'))

    if ENV_VARS.get('HADOOP_HOME') and ENV_VARS.get('HADOOP_HOME') not in sys.path:
        sys.path.append(ENV_VARS.get('HADOOP_HOME'))

def create_spark_session(app_name="SparkDeltaDBT", enable_hive=True):
    """
    Create a Spark session with Delta Lake support.

    Args:
        app_name (str): Name of the Spark application
        enable_hive (bool): Whether to enable Hive support

    Returns:
        SparkSession: Configured Spark session
    """
    logger.info(f"Creating Spark session with app name: {app_name}")

    # Set up environment variables
    setup_environment()

    # Create builder with Delta Lake 3.0.0
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "data", "delta"))
        # Add additional Maven repositories to search for dependencies
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2/,https://packages.delta.io/maven")
        # Add additional configurations to help with Java gateway issues
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{os.path.join(os.getcwd(), 'log4j.properties')} -XX:+UseG1GC")
        .config("spark.executor.extraJavaOptions", f"-Dlog4j.configuration=file:{os.path.join(os.getcwd(), 'log4j.properties')} -XX:+UseG1GC")
        .config("spark.driver.extraClassPath", os.path.join(ENV_VARS.get('SPARK_HOME', ''), "jars", "*"))
        .config("spark.executor.extraClassPath", os.path.join(ENV_VARS.get('SPARK_HOME', ''), "jars", "*"))
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.python.worker.memory", "1g")
        .config("spark.python.worker.reuse", "true")
    )

    # Enable Hive support if requested
    if enable_hive:
        builder = builder.enableHiveSupport()

    # Create session
    spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel(APP_CONFIG['log_level'])

    logger.info("Spark session created successfully")
    return spark

def stop_spark_session(spark):
    """
    Safely stop a Spark session.

    Args:
        spark (SparkSession): The Spark session to stop
    """
    if spark is not None:
        logger.info("Stopping Spark session")
        spark.stop()
        logger.info("Spark session stopped")
