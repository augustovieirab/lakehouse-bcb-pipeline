from datetime import datetime, timedelta
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit


def main():
    spark = SparkSession.builder \
        .appName("bcb-ingestion") \
        .getOrCreate()

    def fetch_bcb_series(codigo, start_date, end_date):
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        
        params = {
            "formato": "json",
            "dataInicial": start_date.strftime("%d/%m/%Y"),
            "dataFinal": end_date.strftime("%d/%m/%Y"),
        }

        response = requests.get(url, params=params, timeout=30)

        if response.status_code != 200:
            raise Exception(f"Erro HTTP {response.status_code}: {response.text}")

        data = response.json()

        if isinstance(data, dict) and "error" in data:
            raise Exception(f"Erro da API: {data['error']}")

        return data

    def fetch_long_period(codigo, start, end):
        all_data = []
        current_start = start

        while current_start < end:
            current_end = min(current_start + timedelta(days=365*10), end)

            data = fetch_bcb_series(codigo, current_start, current_end)
            all_data.extend(data)

            current_start = current_end + timedelta(days=1)

        return all_data

    def get_date_range():
        """ API do Banco Central limita consultas a 10 anos por requisição
        Loop divide o período total em múltiplas chamadas """
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*10)
        return start_date, end_date

    start, end = get_date_range()

    raw_data = fetch_long_period(
        codigo=11,
        start=start,
        end=end
    )

    df = spark.createDataFrame(raw_data)

    df = (
        df
        .withColumn("data", to_date(col("data"), "dd/MM/yyyy"))
        .withColumn("valor", col("valor").cast("double"))
    )

    def add_audit_columns(df, layer="bronze"):
        return (
            df
            .withColumn(f"dt_load_{layer}", lit(datetime.now().strftime("%Y-%m-%d")))
            .withColumn(f"ts_load_{layer}", current_timestamp())
            .withColumn("source", lit("bcb_api"))
            .withColumn("serie_codigo", lit(11))
        )

    df = add_audit_columns(df)

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

    BRONZE_PATH = "data_lake/bronze/bcb/selic"

    write_parquet(
        df,
        path=BRONZE_PATH,
        partition_col="dt_load_bronze"
    )

if __name__ == "__main__":
    main()
