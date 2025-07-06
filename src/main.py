"""
Módulo principal para a aplicação Spark + DBT + Delta Lake.

Este módulo fornece um exemplo simples de como usar o pipeline para processar dados.
"""

import os
import sys
import argparse
from src.pipeline import Pipeline
from src.utils.logger import setup_logger

# Configurar logger
logger = setup_logger(__name__)

def create_sample_data(output_path):
    """
    Criar um arquivo CSV de exemplo para testes.

    Args:
        output_path (str): Caminho para salvar os dados de exemplo

    Returns:
        str: Caminho para os dados de exemplo
    """
    logger.info(f"Creating sample data at {output_path}")

    # Criar diretório de dados se não existir
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Criar dados de exemplo
    with open(output_path, 'w') as f:
        f.write("id,name,age,city,salary\n")
        f.write("1,John Doe,30,New York,75000\n")
        f.write("2,Jane Smith,25,San Francisco,85000\n")
        f.write("3,Bob Johnson,40,Chicago,65000\n")
        f.write("4,Alice Brown,35,Boston,80000\n")
        f.write("5,Charlie Davis,45,Seattle,90000\n")

    logger.info(f"Sample data created at {output_path}")
    return output_path

def parse_args():
    """
    Analisar argumentos da linha de comando.

    Returns:
        argparse.Namespace: Argumentos analisados
    """
    parser = argparse.ArgumentParser(description='Executar o pipeline Spark + DBT + Delta Lake')
    parser.add_argument('--input', type=str, help='Caminho para os dados de entrada')
    parser.add_argument('--type', type=str, default='csv', help='Tipo de dados de entrada (csv, json, parquet)')
    parser.add_argument('--create-sample', action='store_true', help='Criar dados de exemplo para teste')

    return parser.parse_args()

def main():
    """
    Função principal para executar o pipeline.
    """
    # Analisar argumentos da linha de comando
    args = parse_args()

    # Criar dados de exemplo se solicitado
    if args.create_sample:
        sample_path = os.path.join(os.getcwd(), "data", "input", "sample.csv")
        input_path = create_sample_data(sample_path)
    else:
        input_path = args.input

    # Validar caminho de entrada
    if not input_path:
        logger.error("Nenhum caminho de entrada fornecido. Use --input ou --create-sample")
        return

    # Criar e executar o pipeline
    try:
        # Criar pipeline
        pipeline = Pipeline(app_name="SparkDeltaDBT-Example")

        # Definir transformações personalizadas
        def add_salary_category(df):
            """Adicionar uma coluna de categoria de salário baseada no salário."""
            from pyspark.sql import functions as F
            return df.withColumn(
                "salary_category",
                F.when(F.col("salary") < 70000, "Low")
                .when(F.col("salary") < 85000, "Medium")
                .otherwise("High")
            )

        # Executar o pipeline
        pipeline.run_pipeline(
            source_path=input_path,
            source_type=args.type,
            transformations=[add_salary_category]
        )

        logger.info("Pipeline concluído com sucesso")

    except Exception as e:
        logger.error(f"Error running pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    # Configurar variáveis de ambiente
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Executar a função principal
    main()
