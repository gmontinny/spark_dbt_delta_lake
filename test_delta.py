#!/usr/bin/env python3
"""
Script para testar Delta Lake com versões compatíveis.
"""

import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_delta_compatibility():
    """Testa se Delta Lake está funcionando."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        
        print("1. Criando Spark Session com Delta Lake...")
        spark = SparkSession.builder \
            .appName("DeltaTest") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
            .getOrCreate()
        
        print("   OK Spark Session criada")
        
        print("2. Criando DataFrame de teste...")
        data = [(1, "Test", 100), (2, "Delta", 200)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        print("   OK DataFrame criado")
        
        print("3. Salvando em formato Delta...")
        output_path = os.path.join("data", "delta", "test_table")
        df.write.format("delta").mode("overwrite").save(output_path)
        print(f"   OK Salvo em: {output_path}")
        
        print("4. Lendo tabela Delta...")
        df_read = spark.read.format("delta").load(output_path)
        df_read.show()
        print("   OK Tabela Delta lida com sucesso")
        
        spark.stop()
        print("\n✅ Delta Lake funcionando corretamente!")
        return True
        
    except ImportError as e:
        print(f"❌ Erro de importação: {e}")
        print("   Instale as dependências: pip install pyspark==3.5.5 delta-spark==3.0.0")
        return False
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

if __name__ == "__main__":
    # Configurar variáveis de ambiente
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    success = test_delta_compatibility()
    sys.exit(0 if success else 1)