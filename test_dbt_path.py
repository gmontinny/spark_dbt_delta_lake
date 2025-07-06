#!/usr/bin/env python3
"""
Script para testar o DBT com path corrigido.
"""

import os
import sys
import subprocess

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_dbt_access():
    """Testa se o DBT está acessível."""
    try:
        print("1. Testando acesso ao DBT...")
        
        # Configurar environment com DBT path
        env = os.environ.copy()
        env['PATH'] = r'C:\dbt;' + env.get('PATH', '')
        
        # Testar comando DBT
        result = subprocess.run(
            ['dbt', '--version'],
            capture_output=True,
            text=True,
            env=env
        )
        
        if result.returncode == 0:
            print(f"   OK DBT encontrado: {result.stdout.strip()}")
            return True
        else:
            print(f"   ERRO ao executar DBT: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"   ERRO: {e}")
        return False

def test_dbt_runner():
    """Testa o DBTRunner com path corrigido."""
    try:
        print("2. Testando DBTRunner...")
        
        from src.utils.dbt_runner import DBTRunner
        
        # Criar DBTRunner com paths corretos
        dbt_runner = DBTRunner(
            project_dir=r"C:\dbt\models",
            profiles_dir=r"C:\dbt\profiles"
        )
        
        print(f"   OK DBTRunner criado com sucesso")
        print(f"   Project dir: {dbt_runner.project_dir}")
        print(f"   Profiles dir: {dbt_runner.profiles_dir}")
        
        return True
        
    except Exception as e:
        print(f"   ERRO: {e}")
        return False

def main():
    """Função principal."""
    print("=== Teste DBT Path Corrigido ===\n")
    
    success1 = test_dbt_access()
    success2 = test_dbt_runner()
    
    if success1 and success2:
        print("\nDBT configurado corretamente!")
        return True
    else:
        print("\nProblemas encontrados na configuracao DBT")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)