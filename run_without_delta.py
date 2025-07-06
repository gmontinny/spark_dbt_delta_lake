#!/usr/bin/env python3
"""
Script para executar o pipeline sem Delta Lake (apenas Parquet).
"""

import os
import sys
from datetime import datetime

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    """Criar sessão Spark simples sem Delta Lake."""
    return SparkSession.builder \
        .appName("SparkSimple") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

def create_sample_data():
    """Criar dados de exemplo."""
    data = [
        (1, "John Doe", 30, "New York", 75000),
        (2, "Jane Smith", 25, "San Francisco", 85000),
        (3, "Bob Johnson", 40, "Chicago", 65000),
        (4, "Alice Brown", 35, "Boston", 80000),
        (5, "Charlie Davis", 45, "Seattle", 90000)
    ]
    
    columns = ["id", "name", "age", "city", "salary"]
    return data, columns

def main():
    """Função principal."""
    print("=== Pipeline Spark Simples (sem Delta Lake) ===\n")
    
    # Criar sessão Spark
    spark = create_spark_session()
    
    try:
        # Criar dados de exemplo
        data, columns = create_sample_data()
        df = spark.createDataFrame(data, columns)
        
        print("Dados originais:")
        df.show()
        
        # Aplicar transformação
        df_transformed = df.withColumn(
            "salary_category",
            F.when(F.col("salary") < 70000, "Low")
            .when(F.col("salary") < 85000, "Medium")
            .otherwise("High")
        ).withColumn("processed_at", F.current_timestamp())
        
        print("Dados transformados:")
        df_transformed.show()
        
        # Salvar como Parquet
        output_path = os.path.join("data", "processed", f"simple_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df_transformed.write.mode("overwrite").parquet(output_path)
        
        print(f"✅ Dados salvos com sucesso em: {output_path}")
        print("✅ Pipeline executado com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    # Configurar variáveis de ambiente
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    main()