# Correção do Problema de Compatibilidade Delta Lake

## Problema Identificado

O erro `java.lang.NoSuchMethodError: 'scala.collection.Seq org.apache.spark.sql.types.StructType.toAttributes()'` indica **incompatibilidade de versões** entre Spark e Delta Lake.

## Causa Raiz

- **Spark 3.5.5** + **Delta Lake 2.4.0** = INCOMPATÍVEL
- Delta Lake 2.4.0 foi compilado para Spark 3.4.x, não 3.5.x

## Soluções

### Solução 1: Usar Versões Compatíveis (RECOMENDADO)

Atualize o `requirements.txt`:

```txt
# Core dependencies - VERSÕES COMPATÍVEIS
py4j==0.10.9.7
pyspark==3.4.1
delta-spark==2.4.0
dbt-core==1.3.0
dbt-spark==1.3.0

# Utility dependencies
PyYAML==6.0
pandas==1.5.3
numpy==1.24.3
```

### Solução 2: Usar Delta Lake Mais Recente

Ou use Delta Lake mais recente:

```txt
# Core dependencies - VERSÕES MAIS RECENTES
py4j==0.10.9.7
pyspark==3.5.5
delta-spark==3.0.0
dbt-core==1.3.0
dbt-spark==1.3.0
```

### Solução 3: Remover Delta Lake Temporariamente

Para testar sem Delta Lake, modifique `src/utils/spark_session.py`:

```python
def create_spark_session(app_name="SparkDeltaDBT", enable_hive=True):
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Remover configurações Delta Lake temporariamente
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "data", "warehouse"))
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
    )
```

E modifique `src/transformation/data_transformer.py` para usar Parquet:

```python
def save_as_processed(self, df, name):
    # Usar Parquet em vez de Delta
    output_path = os.path.join(self.output_dir, f"{name}_{timestamp}")
    df.write.mode("overwrite").parquet(output_path)
    return output_path
```

## Passos para Correção

### Passo 1: Desinstalar Versões Incompatíveis
```bash
pip uninstall pyspark delta-spark -y
```

### Passo 2: Instalar Versões Compatíveis
```bash
pip install pyspark==3.4.1 delta-spark==2.4.0
```

### Passo 3: Testar
```bash
python run_without_delta.py  # Teste sem Delta Lake primeiro
python run_simple.py --create-sample  # Teste completo
```

## Matriz de Compatibilidade

| Spark Version | Delta Lake Version | Status |
|---------------|-------------------|---------|
| 3.4.x         | 2.4.0            | ✅ Compatível |
| 3.5.x         | 2.4.0            | ❌ Incompatível |
| 3.5.x         | 3.0.0            | ✅ Compatível |
| 3.3.x         | 2.3.0            | ✅ Compatível |

## Verificação de Instalação

Para verificar se as versões estão corretas:

```python
import pyspark
print(f"PySpark: {pyspark.__version__}")

try:
    import delta
    print(f"Delta Lake: {delta.__version__}")
except ImportError:
    print("Delta Lake não instalado")
```

## Próximos Passos

1. Escolha uma das soluções acima
2. Reinstale as dependências com versões compatíveis
3. Execute `python test_structure.py` para verificar
4. Execute `python run_without_delta.py` para teste básico
5. Execute `python run_simple.py --create-sample` para teste completo