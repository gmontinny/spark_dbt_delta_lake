#!/usr/bin/env python3
"""
Script para testar a estrutura do projeto sem dependências externas.
"""

import os
import sys

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Testa se as importações estão funcionando."""
    try:
        print("Testando importações...")
        
        # Testar importação da configuração
        from config.config import APP_CONFIG, ENV_VARS
        print("OK Configuracao importada com sucesso")
        print(f"  - Diretório de entrada: {APP_CONFIG['input_directory']}")
        print(f"  - Nível de log: {APP_CONFIG['log_level']}")
        
        # Testar importação do logger
        from src.utils.logger import setup_logger
        logger = setup_logger(__name__)
        print("OK Logger configurado com sucesso")
        logger.info("Teste de log funcionando")
        
        print("\nTodos os testes de estrutura passaram!")
        return True
        
    except ImportError as e:
        print(f"ERRO de importacao: {e}")
        return False
    except Exception as e:
        print(f"ERRO inesperado: {e}")
        return False

def test_directories():
    """Testa se os diretórios necessários existem."""
    print("\nTestando estrutura de diretórios...")
    
    required_dirs = [
        "data/raw",
        "data/processed", 
        "data/delta",
        "logs",
        "models",
        "src/ingestion",
        "src/transformation",
        "src/utils",
        "config"
    ]
    
    all_exist = True
    for dir_path in required_dirs:
        if os.path.exists(dir_path):
            print(f"OK {dir_path}")
        else:
            print(f"ERRO {dir_path} - nao encontrado")
            all_exist = False
    
    return all_exist

if __name__ == "__main__":
    print("=== Teste de Estrutura do Projeto ===\n")
    
    # Testar diretórios
    dirs_ok = test_directories()
    
    # Testar importações
    imports_ok = test_imports()
    
    if dirs_ok and imports_ok:
        print("\nEstrutura do projeto esta correta!")
        sys.exit(0)
    else:
        print("\nAlguns problemas foram encontrados na estrutura.")
        sys.exit(1)