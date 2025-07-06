#!/usr/bin/env python3
"""
Script para executar o pipeline sem DBT.
"""

import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.main import create_sample_data
from src.pipeline import Pipeline

def main():
    """Função principal sem DBT."""
    print("=== Pipeline sem DBT ===\n")
    
    # Criar dados de exemplo
    sample_path = os.path.join(os.getcwd(), "data", "input", "sample.csv")
    input_path = create_sample_data(sample_path)
    print(f"Dados de exemplo criados: {input_path}")
    
    try:
        # Criar pipeline
        pipeline = Pipeline(app_name="SparkDelta-NoDbt")
        
        # Definir transformação
        def add_salary_category(df):
            from pyspark.sql import functions as F
            return df.withColumn(
                "salary_category",
                F.when(F.col("salary") < 70000, "Low")
                .when(F.col("salary") < 85000, "Medium")
                .otherwise("High")
            )
        
        # Executar apenas ingestion e transformation (sem DBT)
        pipeline.start()
        
        # Ingerir dados
        df, raw_path = pipeline.ingest_data(input_path, "csv")
        print(f"✅ Dados ingeridos: {raw_path}")
        
        # Transformar dados
        df, processed_path = pipeline.transform_data(df, transformations=[add_salary_category])
        print(f"✅ Dados transformados: {processed_path}")
        
        # Mostrar resultado
        print("\n📊 Dados processados:")
        df.show()
        
        pipeline.stop()
        print("\n🎉 Pipeline executado com sucesso (sem DBT)!")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False
    
    return True

if __name__ == "__main__":
    # Configurar variáveis de ambiente
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    success = main()
    sys.exit(0 if success else 1)