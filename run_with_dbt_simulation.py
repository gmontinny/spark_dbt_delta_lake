#!/usr/bin/env python3
"""
Pipeline com simulação DBT.
"""

import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Substituir DBTRunner por versão simulada
class SimulatedDBTRunner:
    def __init__(self, project_dir, profiles_dir):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        print(f"DBT Simulado: project_dir={project_dir}")
        
    def run_models(self, models=None, exclude=None, vars=None, full_refresh=False):
        """Simula execução de modelos DBT."""
        print("🔄 DBT: Iniciando execução de modelos...")
        print("📊 DBT: Processando dados transformados")
        print("🏗️  DBT: Criando tabela employees")
        print("✅ DBT: Modelos executados com sucesso")
        return True

# Substituir o DBTRunner original
import src.utils.dbt_runner
src.utils.dbt_runner.DBTRunner = SimulatedDBTRunner

# Importar e executar pipeline
from src.main import main

if __name__ == "__main__":
    # Configurar variáveis de ambiente
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    print("=== Pipeline com DBT Simulado ===\n")
    
    try:
        # Simular argumentos
        sys.argv = ['run_with_dbt_simulation.py', '--create-sample']
        main()
        print("\n🎉 Pipeline executado com sucesso!")
    except Exception as e:
        print(f"❌ Erro: {e}")
        sys.exit(1)