"""
Módulo para orquestração de todo o pipeline de dados.
"""

import os
import time

from config.config import APP_CONFIG
from src.ingestion.data_reader import DataReader
from src.transformation.data_transformer import DataTransformer
from src.utils.dbt_runner import DBTRunner
from src.utils.logger import setup_logger
from src.utils.spark_session import create_spark_session, stop_spark_session

# Configurar logger
logger = setup_logger(__name__)

class Pipeline:
    """
    Classe para orquestração de todo o pipeline de dados.
    """

    def __init__(self, app_name="SparkDeltaDBT", config=None):
        """
        Inicializa o Pipeline.

        Args:
            app_name (str): Nome da aplicação Spark
            config (dict, optional): Dicionário de configuração
        """
        self.app_name = app_name
        self.config = config or APP_CONFIG
        self.spark = None
        self.reader = None
        self.transformer = None
        self.dbt_runner = None

        logger.info(f"Pipeline inicializado com app_name={app_name}")

    def start(self):
        """
        Inicia o pipeline inicializando todos os componentes.

        Returns:
            bool: True se bem-sucedido, False caso contrário
        """
        logger.info("Iniciando pipeline")

        try:
            # Criar sessão Spark
            self.spark = create_spark_session(app_name=self.app_name)

            # Inicializar componentes
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

            logger.info("Pipeline iniciado com sucesso")
            return True

        except Exception as e:
            logger.error(f"Erro ao iniciar pipeline: {str(e)}")
            self.stop()
            raise

    def stop(self):
        """
        Para o pipeline e limpa os recursos.

        Returns:
            bool: True se bem-sucedido, False caso contrário
        """
        logger.info("Parando pipeline")

        try:
            # Parar sessão Spark
            if self.spark:
                stop_spark_session(self.spark)
                self.spark = None

            logger.info("Pipeline parado com sucesso")
            return True

        except Exception as e:
            logger.error(f"Erro ao parar pipeline: {str(e)}")
            raise

    def ingest_data(self, source_path, source_type="csv", options=None):
        """
        Ingere dados de uma fonte para a camada de dados brutos.

        Args:
            source_path (str): Caminho para os dados de origem
            source_type (str): Tipo de dados de origem (csv, json, parquet)
            options (dict, optional): Opções adicionais para leitura de dados

        Returns:
            tuple: (DataFrame, output_path)
        """
        logger.info(f"Ingerindo dados de {source_path}")

        try:
            # Garantir que o pipeline esteja iniciado
            if not self.spark or not self.reader:
                self.start()

            # Ler dados com base no tipo de origem
            if source_type.lower() == "csv":
                df = self.reader.read_csv(source_path, options=options)
            elif source_type.lower() == "json":
                df = self.reader.read_json(source_path, options=options)
            elif source_type.lower() == "parquet":
                df = self.reader.read_parquet(source_path, options=options)
            else:
                raise ValueError(f"Tipo de origem não suportado: {source_type}")

            # Salvar como dados brutos
            source_name = os.path.basename(source_path).split(".")[0]
            output_path = self.reader.save_as_raw(df, source_name)

            logger.info(f"Dados ingeridos com sucesso de {source_path} para {output_path}")
            return df, output_path

        except Exception as e:
            logger.error(f"Erro ao ingerir dados: {str(e)}")
            raise

    def transform_data(self, df=None, source_path=None, transformations=None, name=None):
        """
        Transforma dados da camada de dados brutos para a camada de dados processados.

        Args:
            df (DataFrame, optional): DataFrame para transformar
            source_path (str, optional): Caminho para os dados brutos
            transformations (list, optional): Lista de funções de transformação
            name (str, optional): Nome do conjunto de dados

        Returns:
            tuple: (DataFrame, output_path)
        """
        logger.info("Transformando dados")

        try:
            # Garantir que o pipeline esteja iniciado
            if not self.spark or not self.transformer:
                self.start()

            # Obter DataFrame se não fornecido
            if df is None and source_path:
                df = self.spark.read.parquet(source_path)
            elif df is None:
                raise ValueError("É necessário fornecer df ou source_path")

            # Limpar dados
            df = self.transformer.clean_data(df)

            # Adicionar colunas de metadados
            source_name = name or (os.path.basename(source_path).split("_")[0] if source_path else "desconhecido")
            df = self.transformer.add_metadata_columns(df, source_name=source_name)

            # Aplicar transformações personalizadas
            if transformations:
                df = self.transformer.apply_transformations(df, transformations)

            # Salvar como dados processados
            output_path = self.transformer.save_as_processed(df, source_name)

            logger.info(f"Dados transformados com sucesso para {output_path}")
            return df, output_path

        except Exception as e:
            logger.error(f"Erro ao transformar dados: {str(e)}")
            raise

    def run_dbt_models(self, models=None, exclude=None, vars=None, full_refresh=False):
        """
        Executa modelos DBT nos dados processados.

        Args:
            models (list, optional): Lista de modelos para executar
            exclude (list, optional): Lista de modelos para excluir
            vars (dict, optional): Variáveis para passar ao DBT
            full_refresh (bool): Se deve fazer uma atualização completa

        Returns:
            bool: True se bem-sucedido, False caso contrário
        """
        logger.info("Executando modelos DBT")

        try:
            # Garantir que o pipeline esteja iniciado
            if not self.dbt_runner:
                self.start()

            # Executar modelos DBT
            success = self.dbt_runner.run_models(
                models=models,
                exclude=exclude,
                vars=vars,
                full_refresh=full_refresh
            )

            if success:
                logger.info("Modelos DBT executados com sucesso")
            else:
                logger.error("Falha ao executar modelos DBT")

            return success

        except Exception as e:
            logger.error(f"Erro ao executar modelos DBT: {str(e)}")
            return False  # Não levanta exceção, apenas retorna False

    def run_pipeline(self, source_path, source_type="csv", transformations=None, dbt_models=None):
        """
        Executa todo o pipeline desde a ingestão até os modelos DBT.

        Args:
            source_path (str): Caminho para os dados de origem
            source_type (str): Tipo de dados de origem (csv, json, parquet)
            transformations (list, optional): Lista de funções de transformação
            dbt_models (list, optional): Lista de modelos DBT para executar

        Returns:
            bool: True se bem-sucedido, False caso contrário
        """
        logger.info(f"Executando pipeline para {source_path}")

        try:
            # Iniciar o pipeline
            self.start()

            # Rastrear tempo de execução
            start_time = time.time()

            # Ingerir dados
            df, raw_path = self.ingest_data(source_path, source_type)

            # Transformar dados
            df, processed_path = self.transform_data(df, transformations=transformations)

            # Executar modelos DBT
            dbt_success = self.run_dbt_models(models=dbt_models)

            # Calcular tempo de execução
            execution_time = time.time() - start_time

            # Registrar sucesso
            logger.info(f"Pipeline concluído em {execution_time:.2f} segundos")
            logger.info(f"Dados brutos: {raw_path}")
            logger.info(f"Dados processados: {processed_path}")
            logger.info(f"Modelos DBT: {'Sucesso' if dbt_success else 'Falha'}")

            # Parar o pipeline
            self.stop()

            return True

        except Exception as e:
            logger.error(f"Erro ao executar pipeline: {str(e)}")
            self.stop()
            raise
