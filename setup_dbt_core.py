#!/usr/bin/env python3
"""
Script para configurar DBT Core local.
"""

import os
import sys
import subprocess

def install_dbt_core():
    """Instala DBT Core."""
    try:
        print("1. Instalando DBT Core...")
        
        # Instalar DBT Core e DBT Spark
        result = subprocess.run([
            sys.executable, '-m', 'pip', 'install', 
            'dbt-core==1.7.0', 'dbt-spark==1.7.0'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("   OK DBT Core instalado")
            return True
        else:
            print(f"   ERRO: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"   ERRO: {e}")
        return False

def test_dbt_core():
    """Testa DBT Core."""
    try:
        print("2. Testando DBT Core...")
        
        result = subprocess.run(['dbt', '--version'], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"   OK DBT Core: {result.stdout.strip()}")
            return True
        else:
            print(f"   ERRO: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"   ERRO: {e}")
        return False

def main():
    """Função principal."""
    print("=== Configuração DBT Core ===\n")
    
    success1 = install_dbt_core()
    success2 = test_dbt_core()
    
    if success1 and success2:
        print("\nDBT Core configurado com sucesso!")
        print("Agora você pode usar: python run_simple.py --create-sample")
        return True
    else:
        print("\nProblemas na configuração DBT Core")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)