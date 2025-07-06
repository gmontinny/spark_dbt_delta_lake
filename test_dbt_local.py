#!/usr/bin/env python3
"""
Teste DBT Core local.
"""

import os
import sys
import subprocess

def test_dbt_local():
    """Testa DBT Core local."""
    try:
        print("=== Teste DBT Core Local ===\n")
        
        # Mudar para diretório models
        os.chdir(os.path.join(os.getcwd(), "models"))
        
        print("1. Testando dbt debug...")
        result = subprocess.run(['dbt', 'debug'], capture_output=True, text=True)
        print(f"   Return code: {result.returncode}")
        if result.stdout:
            print(f"   Stdout: {result.stdout[:200]}...")
        if result.stderr:
            print(f"   Stderr: {result.stderr[:200]}...")
        
        print("\n2. Testando dbt run...")
        result = subprocess.run(['dbt', 'run'], capture_output=True, text=True)
        print(f"   Return code: {result.returncode}")
        if result.stdout:
            print(f"   Stdout: {result.stdout[:200]}...")
        if result.stderr:
            print(f"   Stderr: {result.stderr[:200]}...")
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"Erro: {e}")
        return False

if __name__ == "__main__":
    success = test_dbt_local()
    if success:
        print("\n✅ DBT Core local funcionando!")
    else:
        print("\n❌ Problemas com DBT Core local")
    sys.exit(0 if success else 1)