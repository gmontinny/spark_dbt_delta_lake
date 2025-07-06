"""
Contém todos os parâmetros configuráveis para a aplicação.
"""

import os
import sys


# Variáveis de ambiente
ENV_VARS = {
    'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', sys.executable),
    'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON', sys.executable),
    'JAVA_HOME': os.environ.get('JAVA_HOME', r'C:\Program Files\Java\jdk-11.0.25'),
    'SPARK_HOME': os.environ.get('SPARK_HOME', r'C:\Spark\spark-3.5.5-bin-hadoop3'),
    'HADOOP_HOME': os.environ.get('HADOOP_HOME', r'C:\hadoop')
}

# Configuração da aplicação
APP_CONFIG = {
    'input_directory': os.path.join(os.getcwd(), "data"),
    'log_level': 'INFO',
    'batch_size': 100  # Número de documentos para processar em um lote
}
