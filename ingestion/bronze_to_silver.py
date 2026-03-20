from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
from pyspark.sql.functions import col, to_date
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("bcb-ingestion") \
        .getOrCreate()

    BRONZE_PATH = "data_lake/bronze/bcb/selic"

    df_bronze = spark.read.format("parquet").load(BRONZE_PATH)

    df_silver = (
        df_bronze
        # tipagem correta
        .withColumn("dt_ref", to_date(col("data"), "yyyy-MM-dd"))
        .withColumn("vl_selic", col("valor").cast("double"))
        
        # remover colunas sujas/originais
        .drop("data", "valor")
    )

    # Remove registros duplicados por data, mantendo última ocorrência
    df_silver = df_silver.dropDuplicates(["dt_ref"])

    df_silver = df_silver.orderBy("dt_ref")

    df_silver = df_silver.filter(
        col("dt_ref").isNotNull() & col("vl_selic").isNotNull()
    )

    df_silver = (
        df_silver
        .withColumn("dt_load_silver", lit(datetime.now().strftime("%Y-%m-%d")))
        .withColumn("ts_load_silver", current_timestamp())
    )

    SILVER_PATH = "data_lake/silver/bcb/selic"

    def write_parquet(df, path, mode="append", partition_col=None):
        writer = (
            df.write
            .format("parquet")
            .mode(mode)
        )
        
        if partition_col:
            writer = writer.partitionBy(partition_col)
        
        writer.save(path)
        print(f"Writing data to {path} (mode={mode})")

    write_parquet(
        df_silver,
        path=SILVER_PATH,
        partition_col="dt_load_silver"
    )

if __name__ == "__main__":
    main()