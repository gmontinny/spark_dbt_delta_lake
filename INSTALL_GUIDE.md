# Guia de Instalação e Correção de Problemas

## Problemas Corrigidos

### 1. Arquivo requirements.txt Corrompido
- **Problema**: O arquivo estava com codificação incorreta (UTF-16)
- **Solução**: Reescrito em UTF-8 com as dependências corretas

### 2. Estrutura de Pacotes Python
- **Problema**: Faltavam arquivos `__init__.py` nos diretórios
- **Solução**: Adicionados arquivos `__init__.py` em todos os pacotes:
  - `src/__init__.py`
  - `src/utils/__init__.py`
  - `src/ingestion/__init__.py`
  - `src/transformation/__init__.py`
  - `config/__init__.py`

### 3. Problemas de Importação
- **Problema**: Módulos não eram encontrados
- **Solução**: Criado script `run_simple.py` que configura o path corretamente

## Pré-requisitos

### 1. Java
Instale o Java 8 ou 11:
```bash
# Verificar se Java está instalado
java -version

# Se não estiver instalado, baixe de:
# https://adoptium.net/
```

### 2. Python
Certifique-se de ter Python 3.7+ instalado:
```bash
python --version
```

### 3. Pip
Verifique se o pip está funcionando:
```bash
python -m pip --version
```

## Instalação

### Passo 1: Instalar Dependências
```bash
# Navegar para o diretório do projeto
cd c:\projeto-python\spark\spark_dbt_delta_lake

# Instalar dependências
python -m pip install -r requirements.txt
```

### Passo 2: Configurar Variáveis de Ambiente
Edite o arquivo `config/config.py` e ajuste os caminhos:

```python
ENV_VARS = {
    'JAVA_HOME': r'C:\Program Files\Java\jdk-11.0.25',  # Ajuste para seu Java
    'SPARK_HOME': r'C:\Spark\spark-3.5.5-bin-hadoop3', # Ajuste para seu Spark
    'HADOOP_HOME': r'C:\hadoop'                         # Ajuste para seu Hadoop
}
```

### Passo 3: Baixar e Configurar Spark
Se não tiver o Spark instalado:

1. Baixe o Spark 3.5.5 de: https://spark.apache.org/downloads.html
2. Extraia para `C:\Spark\spark-3.5.5-bin-hadoop3`
3. Baixe o Hadoop de: https://hadoop.apache.org/releases.html
4. Extraia para `C:\hadoop`

## Execução

### Teste de Estrutura (sem dependências externas)
```bash
python test_structure.py
```

### Execução Completa (com Spark)
```bash
# Usando o script simples
python run_simple.py --create-sample

# Ou usando o módulo diretamente (após correções)
python -m src.main --create-sample
```

### Verificação da Aplicação
```bash
python verify_application.py
```

## Solução de Problemas Comuns

### Erro: "No module named 'pyspark'"
```bash
python -m pip install pyspark==3.4.1
```

### Erro: Delta Lake Incompatibilidade (NoSuchMethodError)
**SOLUÇÃO IMPLEMENTADA**: Configurado para usar versões compatíveis!

**Versões Configuradas**:
```bash
pip install pyspark==3.5.5 delta-spark==3.0.0
```

**Teste Delta Lake**:
```bash
python test_delta.py
```

**Solução 3** - Execute sem Delta Lake:
```bash
python run_without_delta.py
```

### Erro: "JAVA_HOME is not set"
1. Instale o Java
2. Configure a variável JAVA_HOME no `config/config.py`

### Erro: "py4j.protocol.Py4JJavaError"
1. Verifique se o Java está na versão correta (8 ou 11)
2. Verifique se o SPARK_HOME está configurado corretamente

### Erro de Codificação no Windows
Se encontrar problemas de codificação, execute:
```bash
chcp 65001
```

## Scripts Disponíveis

- `test_structure.py` - Testa a estrutura sem dependências externas
- `test_delta.py` - **NOVO**: Testa compatibilidade Delta Lake 3.0.0
- `run_without_delta.py` - Executa sem Delta Lake (apenas Parquet)
- `run_simple.py` - Executa o pipeline com configuração simplificada
- `verify_application.py` - Verifica se tudo está funcionando
- `run_tests.py` - Executa os testes unitários

## Próximos Passos

1. Execute `python test_structure.py` para verificar a estrutura
2. Instale as dependências com `python -m pip install -r requirements.txt`
3. Configure os caminhos no `config/config.py`
4. Execute `python run_simple.py --create-sample` para testar

## Suporte

Se ainda encontrar problemas:
1. Verifique os logs na pasta `logs/`
2. Execute `python test_structure.py` para diagnóstico
3. Verifique se todas as dependências estão instaladas