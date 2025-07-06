"""
Módulo para ler dados de várias fontes e armazená-los no diretório de dados brutos.
"""

import os
import json
import csv
from datetime import datetime
from pyspark.sql import DataFrame
from src.utils.logger import setup_logger

# Configurar logger
logger = setup_logger(__name__)

class DataReader:
    """
    Classe para ler dados de várias fontes e armazená-los no diretório de dados brutos.
    """

    def __init__(self, spark, input_dir=None, output_dir=None):
        """
        Inicializar o DataReader.

        Args:
            spark: Objeto SparkSession
            input_dir (str, optional): Diretório para ler dados
            output_dir (str, optional): Diretório para escrever dados brutos
        """
        self.spark = spark
        self.input_dir = input_dir or os.path.join(os.getcwd(), "data", "input")
        self.output_dir = output_dir or os.path.join(os.getcwd(), "data", "raw")

        # Garantir que o diretório de saída exista
        os.makedirs(self.output_dir, exist_ok=True)

        logger.info(f"DataReader initialized with input_dir={self.input_dir}, output_dir={self.output_dir}")

    def read_csv(self, file_path, header=True, infer_schema=True, options=None):
        """
        Ler um arquivo CSV em um DataFrame Spark.

        Args:
            file_path (str): Caminho para o arquivo CSV
            header (bool): Se o arquivo CSV tem um cabeçalho
            infer_schema (bool): Se deve inferir o esquema
            options (dict, optional): Opções adicionais para leitura de CSV

        Returns:
            DataFrame: DataFrame Spark contendo os dados CSV
        """
        logger.info(f"Reading CSV file: {file_path}")

        try:
            # Definir opções padrão
            read_options = {
                "header": str(header).lower(),
                "inferSchema": str(infer_schema).lower()
            }

            # Adicionar opções adicionais se fornecidas
            if options:
                read_options.update(options)

            # Ler o arquivo CSV
            df = self.spark.read.options(**read_options).csv(file_path)

            # Registrar sucesso
            logger.info(f"Successfully read CSV file with {df.count()} rows and {len(df.columns)} columns")

            return df

        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            raise

    def read_json(self, file_path, multiline=False, options=None):
        """
        Ler um arquivo JSON em um DataFrame Spark.

        Args:
            file_path (str): Caminho para o arquivo JSON
            multiline (bool): Se o arquivo JSON tem múltiplos objetos JSON por linha
            options (dict, optional): Opções adicionais para leitura de JSON

        Returns:
            DataFrame: DataFrame Spark contendo os dados JSON
        """
        logger.info(f"Reading JSON file: {file_path}")

        try:
            # Definir opções padrão
            read_options = {
                "multiline": str(multiline).lower()
            }

            # Adicionar opções adicionais se fornecidas
            if options:
                read_options.update(options)

            # Ler o arquivo JSON
            df = self.spark.read.options(**read_options).json(file_path)

            # Registrar sucesso
            logger.info(f"Successfully read JSON file with {df.count()} rows and {len(df.columns)} columns")

            return df

        except Exception as e:
            logger.error(f"Error reading JSON file: {str(e)}")
            raise

    def read_parquet(self, file_path, options=None):
        """
        Ler um arquivo Parquet em um DataFrame Spark.

        Args:
            file_path (str): Caminho para o arquivo Parquet
            options (dict, optional): Opções adicionais para leitura de Parquet

        Returns:
            DataFrame: DataFrame Spark contendo os dados Parquet
        """
        logger.info(f"Reading Parquet file: {file_path}")

        try:
            # Definir opções padrão
            read_options = {}

            # Adicionar opções adicionais se fornecidas
            if options:
                read_options.update(options)

            # Ler o arquivo Parquet
            df = self.spark.read.options(**read_options).parquet(file_path)

            # Registrar sucesso
            logger.info(f"Successfully read Parquet file with {df.count()} rows and {len(df.columns)} columns")

            return df

        except Exception as e:
            logger.error(f"Error reading Parquet file: {str(e)}")
            raise

    def save_as_raw(self, df, name, format="parquet", partition_by=None, mode="overwrite"):
        """
        Save a DataFrame as raw data.

        Args:
            df (DataFrame): Spark DataFrame to save
            name (str): Name of the dataset
            format (str): Format to save as (parquet, delta, csv, json)
            partition_by (list, optional): Columns to partition by
            mode (str): Write mode (overwrite, append, ignore, error)

        Returns:
            str: Path where the data was saved
        """
        # Create timestamp for versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create output path
        output_path = os.path.join(self.output_dir, f"{name}_{timestamp}")

        logger.info(f"Saving DataFrame as raw data: {output_path}")

        try:
            # Create writer
            writer = df.write.format(format).mode(mode)

            # Add partitioning if specified
            if partition_by:
                writer = writer.partitionBy(partition_by)

            # Save the data
            writer.save(output_path)

            # Log success
            logger.info(f"Successfully saved DataFrame with {df.count()} rows to {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Error saving DataFrame: {str(e)}")
            raise
