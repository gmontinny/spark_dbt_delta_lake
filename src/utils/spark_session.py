"""
Módulo utilitário para criar e gerenciar sessões Spark com suporte a Delta Lake.
"""

import os
import sys
from pyspark.sql import SparkSession
from config.config import ENV_VARS, APP_CONFIG
from src.utils.logger import setup_logger

# Configurar logger
logger = setup_logger(__name__)

def setup_environment():
    """
    Configurar variáveis de ambiente necessárias para Spark e Delta Lake.
    """
    logger.info("Configurando variáveis de ambiente para Spark")

    # Definir variáveis de ambiente
    for key, value in ENV_VARS.items():
        if value:
            os.environ[key] = value
            logger.debug(f"Variável de ambiente definida {key}={value}")

    # Adicionar Spark e Hadoop ao caminho do sistema se necessário
    if ENV_VARS.get('SPARK_HOME') and ENV_VARS.get('SPARK_HOME') not in sys.path:
        sys.path.append(ENV_VARS.get('SPARK_HOME'))

    if ENV_VARS.get('HADOOP_HOME') and ENV_VARS.get('HADOOP_HOME') not in sys.path:
        sys.path.append(ENV_VARS.get('HADOOP_HOME'))

def create_spark_session(app_name="SparkDeltaDBT", enable_hive=True):
    """
    Criar uma sessão Spark com suporte a Delta Lake.

    Args:
        app_name (str): Nome da aplicação Spark
        enable_hive (bool): Se deve habilitar suporte a Hive

    Returns:
        SparkSession: Sessão Spark configurada
    """
    logger.info(f"Criando sessão Spark com nome da aplicação: {app_name}")

    # Configurar variáveis de ambiente
    setup_environment()

    # Criar builder com Delta Lake 3.0.0
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "data", "delta"))
        # Adicionar repositórios Maven adicionais para buscar dependências
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2/,https://packages.delta.io/maven")
        # Adicionar configurações adicionais para ajudar com problemas de gateway Java
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

    # Habilitar suporte a Hive se solicitado
    if enable_hive:
        builder = builder.enableHiveSupport()

    # Criar sessão
    spark = builder.getOrCreate()

    # Definir nível de log
    spark.sparkContext.setLogLevel(APP_CONFIG['log_level'])

    logger.info("Sessão Spark criada com sucesso")
    return spark

def stop_spark_session(spark):
    """
    Parar com segurança uma sessão Spark.

    Args:
        spark (SparkSession): A sessão Spark a ser parada
    """
    if spark is not None:
        logger.info("Parando sessão Spark")
        spark.stop()
        logger.info("Sessão Spark parada")
