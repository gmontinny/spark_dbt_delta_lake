"""
Módulo para transformar dados brutos em dados processados usando transformações Spark.
"""

import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.logger import setup_logger

# Configurar logger
logger = setup_logger(__name__)

class DataTransformer:
    """
    Classe para transformar dados brutos em dados processados usando transformações Spark.
    """

    def __init__(self, spark, input_dir=None, output_dir=None):
        """
        Inicializar o DataTransformer.

        Args:
            spark: Objeto SparkSession
            input_dir (str, optional): Diretório para ler dados brutos
            output_dir (str, optional): Diretório para escrever dados processados
        """
        self.spark = spark
        self.input_dir = input_dir or os.path.join(os.getcwd(), "data", "raw")
        self.output_dir = output_dir or os.path.join(os.getcwd(), "data", "processed")

        # Garantir que o diretório de saída exista
        os.makedirs(self.output_dir, exist_ok=True)

        logger.info(f"DataTransformer initialized with input_dir={self.input_dir}, output_dir={self.output_dir}")

    def clean_data(self, df, columns_to_drop=None, null_threshold=0.8):
        """
        Limpar um DataFrame removendo colunas com muitos valores nulos e tratando valores ausentes.

        Args:
            df (DataFrame): DataFrame Spark para limpar
            columns_to_drop (list, optional): Colunas para remover
            null_threshold (float): Limite para remoção de colunas com muitos valores nulos

        Returns:
            DataFrame: DataFrame limpo
        """
        logger.info("Cleaning DataFrame")

        try:
            # Fazer uma cópia do DataFrame
            result_df = df

            # Remover colunas especificadas
            if columns_to_drop:
                result_df = result_df.drop(*columns_to_drop)
                logger.info(f"Dropped columns: {columns_to_drop}")

            # Calcular percentuais de valores nulos para cada coluna
            total_rows = result_df.count()
            if total_rows > 0:
                for column in result_df.columns:
                    null_count = result_df.filter(F.col(column).isNull()).count()
                    null_percentage = null_count / total_rows

                    # Remover colunas com muitos valores nulos
                    if null_percentage > null_threshold:
                        result_df = result_df.drop(column)
                        logger.info(f"Dropped column {column} with {null_percentage:.2%} nulls")

            # Registrar sucesso
            logger.info(f"Successfully cleaned DataFrame, removed {len(df.columns) - len(result_df.columns)} columns")

            return result_df

        except Exception as e:
            logger.error(f"Error cleaning DataFrame: {str(e)}")
            raise

    def add_metadata_columns(self, df, source_name=None):
        """
        Adicionar colunas de metadados a um DataFrame.

        Args:
            df (DataFrame): DataFrame Spark para adicionar metadados
            source_name (str, optional): Nome da fonte de dados

        Returns:
            DataFrame: DataFrame com colunas de metadados
        """
        logger.info("Adding metadata columns to DataFrame")

        try:
            # Adicionar timestamp de processamento
            result_df = df.withColumn("processing_time", F.current_timestamp())

            # Adicionar nome da fonte se fornecido
            if source_name:
                result_df = result_df.withColumn("source_name", F.lit(source_name))

            # Adicionar número da linha
            window_spec = Window.orderBy(F.lit(1))
            result_df = result_df.withColumn("row_id", F.row_number().over(window_spec))

            # Registrar sucesso
            logger.info("Successfully added metadata columns to DataFrame")

            return result_df

        except Exception as e:
            logger.error(f"Error adding metadata columns: {str(e)}")
            raise

    def apply_transformations(self, df, transformations):
        """
        Aplicar uma lista de transformações a um DataFrame.

        Args:
            df (DataFrame): DataFrame Spark para transformar
            transformations (list): Lista de funções de transformação para aplicar

        Returns:
            DataFrame: DataFrame transformado
        """
        logger.info(f"Applying {len(transformations)} transformations to DataFrame")

        try:
            result_df = df

            # Aplicar cada transformação em sequência
            for i, transformation in enumerate(transformations):
                logger.info(f"Applying transformation {i+1}/{len(transformations)}")
                result_df = transformation(result_df)

            # Registrar sucesso
            logger.info("Successfully applied all transformations to DataFrame")

            return result_df

        except Exception as e:
            logger.error(f"Error applying transformations: {str(e)}")
            raise

    def save_as_processed(self, df, name, format="delta", partition_by=None, mode="overwrite"):
        """
        Salvar um DataFrame como dados processados.

        Args:
            df (DataFrame): DataFrame Spark para salvar
            name (str): Nome do conjunto de dados
            format (str): Formato para salvar (parquet, delta, csv, json)
            partition_by (list, optional): Colunas para particionar
            mode (str): Modo de escrita (overwrite, append, ignore, error)

        Returns:
            str: Caminho onde os dados foram salvos
        """
        # Criar timestamp para versionamento
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Criar caminho de saída
        output_path = os.path.join(self.output_dir, f"{name}_{timestamp}")

        logger.info(f"Saving DataFrame as processed data: {output_path}")

        try:
            # Criar escritor
            writer = df.write.format(format).mode(mode)

            # Adicionar particionamento se especificado
            if partition_by:
                writer = writer.partitionBy(partition_by)

            # Salvar os dados
            writer.save(output_path)

            # Registrar sucesso
            logger.info(f"Successfully saved DataFrame with {df.count()} rows to {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Error saving DataFrame: {str(e)}")
            raise
