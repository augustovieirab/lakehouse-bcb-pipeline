# 🏛️ Deep Dive Arquitetural: Lakehouse Pipeline (SELIC)

Este documento detalha as decisões de engenharia, o fluxo de dados e os trade-offs assumidos na construção do pipeline Lakehouse para os dados da taxa SELIC do Banco Central. A premissa central é manter uma arquitetura **desacoplada, idempotente e pronta para escalar**.

## 🔄 O Fluxo de Dados (Data Flow)

O pipeline foi desenhado para garantir que o processamento pesado (I/O e tipagem) seja isolado da modelagem de negócio (Analytics).

```mermaid
sequenceDiagram
    participant API as Banco Central (API)
    participant Ingest as Python Ingestion
    participant Bronze as Data Lake (Bronze)
    participant Spark as PySpark Engine
    participant Silver as Data Lake (Silver)
    participant dbt as dbt (Analytics)
    participant DuckDB as DuckDB Engine

    Ingest->>API: Requisição paginada (Janelas < 10 anos)
    API-->>Ingest: JSON (Dados brutos)
    Ingest->>Bronze: Salva Parquet (Raw + Metadados)
    Spark->>Bronze: Lê dados brutos
    Spark->>Spark: Cast de tipos, Deduplicação, Schema Enforcement
    Spark->>Silver: Salva Parquet (Clean & Typed)
    dbt->>Silver: Lê dados tratados via DuckDB
    dbt->>dbt: Aplica regras de negócio (Staging, Intermediate, Marts)
    dbt-->>DuckDB: Disponibiliza Views/Tabelas para Consumo
🥞 Design de Camadas (Arquitetura Medallion)
Para ser didático, pense neste pipeline como uma Estação de Tratamento de Água:

🥉 Camada Bronze (A Represa)
Aqui armazenamos a "água bruta", exatamente como veio da natureza (API).

Responsabilidade: Ingestão puramente extrativa via Python (requests). Gerencia a paginação rigorosa da API do BCB.

Persistência: Arquivos Parquet append-only. Se o pipeline quebrar na frente, não precisamos bater na API novamente.

Metadados: Injeção de dt_load_bronze e ts_load_bronze para garantir rastreabilidade da linhagem (data lineage).

🥈 Camada Silver (A Estação de Filtragem)
Aqui a água é tratada, filtrada e padronizada para consumo seguro.

Responsabilidade: Atuação pesada do PySpark.

Transformações: Type casting rígido (strings para date e double), Schema Enforcement (garantindo que o formato do dado não mude de surpresa) e deduplicação de registros.

Saída: Dados estruturados, limpos e prontos para exploração ad-hoc.

🥇 Camada Gold / Analytics (A Água Mineral Enriquecida)
A água agora recebe vitaminas e é engarrafada para o cliente final.

Responsabilidade: Modelagem dimensional gerenciada 100% pelo dbt.

Staging: Padronização leve e renomeação de colunas.

Intermediate: Cálculo de métricas complexas (médias móveis, variações percentuais).

Marts: Tabelas finais, agregadas e prontas para plugar em um painel de BI.

⚖️ Decisões de Design & Trade-offs
Em engenharia de software, não existe solução perfeita, apenas trade-offs. Abaixo, o racional das escolhas deste projeto:

1. Separação de Concerns: Spark vs. dbt
A Decisão: Usar Spark para o ETL pesado (Bronze para Silver) e dbt para a transformação semântica (Silver para Gold).

O Porquê: Evita sobrecarga de responsabilidades. O Spark é o motor ideal para lidar com arquivos sujos e I/O intensivo. O dbt brilha ao aplicar regras de negócio usando SQL padrão. Isso permite que um Analytics Engineer crie métricas no dbt sem precisar saber programar em PySpark.

2. Formato Parquet Local vs. Bancos de Dados Tradicionais
A Decisão: Armazenar dados em Parquet particionados por data de carga no sistema de arquivos local.

O Porquê: Formatos colunares oferecem compressão agressiva e performance de leitura superior. Aliado ao DuckDB, conseguimos consultar o Data Lake localmente executando agregações complexas em milissegundos, atingindo uma arquitetura de "Zero-Infra".

3. Trade-offs Assumidos (Conscientes)
Execução Local (Single-node): Limita a escala horizontal, mas foi escolhida para manter a barreira de entrada baixa e focar no desenho arquitetural.

Ausência de Orquestrador (Airflow/Dagster): A execução atual é manual via Makefile. Adicionar um orquestrador agora traria um overhead de infraestrutura desnecessário para o MVP.

Parquet vs. Delta Lake: O Parquet puro não suporta operações DML ACID (Merge/Update/Delete). A estratégia atual exige overwrite de partições.

🚀 Caminho de Evolução (Future State)
A arquitetura foi desenhada de forma modular para permitir evolução sem refatoração do código base:

Storage Engine: Substituir o gravador Parquet pelo formato Delta Lake, habilitando Time Travel e Liquid Clustering para otimização de leitura.

Orquestração: Migrar a execução do Makefile para DAGs no Apache Airflow, gerenciando dependências, alertas e retries automatizados.

Cloud Native: Fazer o lift-and-shift do storage local para o Amazon S3, substituindo o motor local pelo AWS Glue (PySpark) e AWS Athena (Queries analíticas).

Governança: Adicionar rotinas de Data Quality usando testes nativos do dbt e Great Expectations no PySpark.