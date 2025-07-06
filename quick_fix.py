#!/usr/bin/env python3
"""
Script de correção rápida - simula o pipeline sem dependências externas.
"""

import os
import sys
import json
from datetime import datetime

def create_sample_data():
    """Criar dados de exemplo."""
    data = [
        {"id": 1, "name": "John Doe", "age": 30, "city": "New York", "salary": 75000},
        {"id": 2, "name": "Jane Smith", "age": 25, "city": "San Francisco", "salary": 85000},
        {"id": 3, "name": "Bob Johnson", "age": 40, "city": "Chicago", "salary": 65000},
        {"id": 4, "name": "Alice Brown", "age": 35, "city": "Boston", "salary": 80000},
        {"id": 5, "name": "Charlie Davis", "age": 45, "city": "Seattle", "salary": 90000}
    ]
    return data

def transform_data(data):
    """Aplicar transformações aos dados."""
    for record in data:
        # Adicionar categoria de salário
        if record["salary"] < 70000:
            record["salary_category"] = "Low"
        elif record["salary"] < 85000:
            record["salary_category"] = "Medium"
        else:
            record["salary_category"] = "High"
        
        # Adicionar timestamp
        record["processed_at"] = datetime.now().isoformat()
    
    return data

def save_data(data, output_path):
    """Salvar dados em JSON."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    return output_path

def main():
    """Função principal."""
    print("=== Pipeline Simulado (sem Spark) ===\n")
    
    try:
        # Criar dados de exemplo
        print("1. Criando dados de exemplo...")
        data = create_sample_data()
        print(f"   OK {len(data)} registros criados")
        
        # Transformar dados
        print("2. Aplicando transformações...")
        transformed_data = transform_data(data)
        print("   OK Transformacoes aplicadas")
        
        # Salvar dados
        print("3. Salvando dados processados...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join("data", "processed", f"simulated_output_{timestamp}.json")
        saved_path = save_data(transformed_data, output_path)
        print(f"   OK Dados salvos em: {saved_path}")
        
        # Mostrar resultado
        print("\n4. Resultado:")
        for record in transformed_data:
            print(f"   {record['name']} - {record['salary_category']} ({record['salary']})")
        
        print(f"\nPipeline simulado executado com sucesso!")
        print(f"Arquivo salvo: {saved_path}")
        
        return True
        
    except Exception as e:
        print(f"ERRO: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)