# Integração Spark + DBT + Delta Lake

Este projeto demonstra uma aplicação Apache Spark local integrada com DBT Core e Delta Lake, seguindo princípios de código limpo e melhores práticas.

## Arquitetura

O projeto segue uma arquitetura modular com os seguintes componentes:

1. **Ingestão de Dados**: Lê dados de várias fontes (CSV, JSON, Parquet) e armazena na camada de dados brutos.
2. **Transformação de Dados**: Transforma dados brutos usando transformações Spark e armazena na camada de dados processados.
3. **Integração DBT**: Usa DBT Core para modelagem e transformação de dados em formato Delta Lake.
4. **Orquestração de Pipeline**: Coordena todo o pipeline de dados desde a ingestão até os modelos DBT.

### Estrutura de Diretórios

```
spark_dbt_delta_lake/
├── config/             # Arquivos de configuração
├── data/               # Diretórios de dados
│   ├── raw/            # Dados brutos (Delta Lake)
│   ├── processed/      # Dados processados (Delta Lake)
│   └── delta/          # Tabelas Delta Lake
├── logs/               # Logs da aplicação
├── models/             # Modelos DBT
│   ├── dbt_project.yml # Configuração do projeto DBT
│   ├── employees.sql   # Modelo principal
│   ├── raw_employees.sql # Modelo base
│   └── sources.yml     # Definições de fontes Delta Lake
├── src/                # Código fonte
│   ├── ingestion/      # Módulos de ingestão de dados
│   ├── transformation/ # Módulos de transformação de dados
│   ├── utils/          # Módulos utilitários
│   ├── main.py         # Aplicação principal
│   └── pipeline.py     # Orquestração do pipeline
├── tests/              # Scripts de teste
├── run_simple.py       # Script para executar o pipeline
├── install_dbt_core.py # Script para instalar DBT Core
└── README.md           # Documentação do projeto
```

## Funcionalidades

- **Integração Spark**: Usa Apache Spark para processamento distribuído de dados.
- **Suporte Delta Lake**: Armazena dados em formato Delta Lake para transações ACID e time travel.
- **Integração DBT Core**: Usa DBT Core local para modelagem e transformação de dados.
- **Design Modular**: Segue um design modular para fácil manutenção e extensão.
- **Tratamento de Erros**: Inclui tratamento abrangente de erros e logging.
- **Gerenciamento de Configuração**: Usa um sistema de configuração centralizado.
- **Código Limpo**: Segue princípios de código limpo e melhores práticas.

## Requisitos

- Python 3.7+
- Apache Spark 3.x
- Delta Lake 3.0+
- DBT Core 1.7+
- Java 8 ou 11

## Instalação

### 1. Clonar o repositório
```bash
git clone https://github.com/seuusuario/spark_dbt_delta_lake.git
cd spark_dbt_delta_lake
```

### 2. Instalar dependências
```bash
pip install -r requirements.txt
```

### 3. Instalar DBT Core
```bash
python install_dbt_core.py
```

### 4. Configurar variáveis de ambiente
Edite o arquivo `config/config.py` e ajuste os caminhos:

```python
ENV_VARS = {
    'JAVA_HOME': r'C:\Program Files\Java\jdk-11.0.25',  # Ajuste para seu Java
    'SPARK_HOME': r'C:\Spark\spark-3.5.5-bin-hadoop3', # Ajuste para seu Spark
    'HADOOP_HOME': r'C:\hadoop'                         # Ajuste para seu Hadoop
}
```

## Uso

### Uso Básico

Executar a aplicação com dados de exemplo:

```bash
python run_simple.py --create-sample
```

### Entrada Personalizada

Fornecer seus próprios dados de entrada:

```bash
python run_simple.py --input /caminho/para/seus/dados.csv --type csv
```

### Uso Avançado

Para uso mais avançado, você pode importar a classe `Pipeline` em seu próprio código:

```python
from src.pipeline import Pipeline

# Criar pipeline
pipeline = Pipeline(app_name="SeuAppName")

# Definir transformações personalizadas
def sua_transformacao(df):
    # Sua lógica de transformação aqui
    return df

# Executar o pipeline
pipeline.run_pipeline(
    source_path="/caminho/para/seus/dados.csv",
    source_type="csv",
    transformations=[sua_transformacao]
)
```

## Desenvolvimento

### Adicionando Novas Fontes de Dados

Para adicionar suporte a uma nova fonte de dados, estenda a classe `DataReader` em `src/ingestion/data_reader.py`:

```python
def read_new_format(self, file_path, options=None):
    # Implementação para ler o novo formato
    pass
```

### Adicionando Novas Transformações

Crie funções de transformação personalizadas que recebem um DataFrame e retornam um DataFrame transformado:

```python
def minha_transformacao(df):
    # Sua lógica de transformação aqui
    return df_transformado
```

### Adicionando Novos Modelos DBT

Crie novos modelos DBT no diretório `models` seguindo a estrutura do projeto DBT:

```sql
-- models/meu_modelo.sql
{{ config(materialized='table', file_format='delta') }}

SELECT 
    *
FROM delta.`data/processed`
WHERE condicao = 'valor'
```

## Testes

O projeto inclui uma suíte de testes abrangente para verificar se a aplicação está funcionando corretamente.

### Executando Testes

Para executar os testes, execute o seguinte comando no diretório raiz do projeto:

```bash
python run_tests.py
```

### Verificação Rápida

Para uma verificação rápida de que a aplicação está funcionando corretamente:

```bash
python test_structure.py
```

### Scripts de Teste Disponíveis

- `test_structure.py` - Testa a estrutura sem dependências externas
- `test_delta.py` - Testa compatibilidade Delta Lake
- `test_dbt_local.py` - Testa DBT Core local
- `quick_fix.py` - Pipeline simulado sem dependências

## Configuração DBT

### Profiles DBT

O arquivo `~/.dbt/profiles.yml` está configurado para usar Spark com Delta Lake:

```yaml
spark_dbt_delta_lake:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      schema: default
      threads: 1
      host: localhost
      port: 10000
```

### Modelos DBT

Os modelos DBT estão configurados para usar Delta Lake como formato padrão:

```sql
{{ config(materialized='table', file_format='delta') }}
```

## Fluxo de Dados

```
1. Ingestão (CSV/JSON/Parquet → Delta Lake)
2. Transformação (Spark → Delta Lake)
3. Modelagem DBT (Delta Lake → Delta Lake)
4. Saída Final (Tabelas Delta Lake prontas)
```

## Solução de Problemas

### Erro: "No module named 'pyspark'"
```bash
pip install pyspark==3.5.5 delta-spark==3.0.0
```

### Erro: "JAVA_HOME is not set"
1. Instale o Java 8 ou 11
2. Configure a variável JAVA_HOME no `config/config.py`

### Erro DBT
```bash
# Reinstalar DBT Core
python install_dbt_core.py

# Testar DBT
python test_dbt_local.py
```

## Scripts Disponíveis

- `run_simple.py` - Executa o pipeline completo
- `install_dbt_core.py` - Instala DBT Core local
- `test_dbt_local.py` - Testa DBT Core
- `test_delta.py` - Testa Delta Lake
- `test_structure.py` - Testa estrutura do projeto

## Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo LICENSE para detalhes.

## Contribuição

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## Status do Projeto

✅ **Spark + Delta Lake**: Funcionando  
✅ **DBT Core Local**: Configurado  
✅ **Pipeline Completo**: Operacional  
✅ **Testes**: Disponíveis  
✅ **Documentação**: Atualizada