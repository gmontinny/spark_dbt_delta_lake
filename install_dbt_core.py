#!/usr/bin/env python3
"""
Script para instalar DBT Core local.
"""

import subprocess
import sys
import os

def install_dbt_core():
    """Instala DBT Core usando pip direto."""
    try:
        print("Instalando DBT Core...")
        
        # Comandos de instalação
        packages = [
            'dbt-core==1.7.0',
            'dbt-spark==1.7.0'
        ]
        
        for package in packages:
            print(f"Instalando {package}...")
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install', package
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"Erro instalando {package}: {result.stderr}")
                return False
        
        print("DBT Core instalado com sucesso!")
        return True
        
    except Exception as e:
        print(f"Erro: {e}")
        return False

def test_dbt():
    """Testa DBT Core."""
    try:
        result = subprocess.run(['dbt', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"DBT funcionando: {result.stdout}")
            return True
        else:
            print(f"Erro testando DBT: {result.stderr}")
            return False
    except Exception as e:
        print(f"DBT não encontrado: {e}")
        return False

if __name__ == "__main__":
    if install_dbt_core():
        if test_dbt():
            print("✅ DBT Core configurado com sucesso!")
        else:
            print("❌ DBT instalado mas não funcionando")
    else:
        print("❌ Falha na instalação do DBT Core")