#!/usr/bin/env python3
"""
Script simples para executar o pipeline sem problemas de importação.
"""

import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Importar e executar o main
from src.main import main

if __name__ == "__main__":
    # Configurar variáveis de ambiente
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Executar o main
    main()