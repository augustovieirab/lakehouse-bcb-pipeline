from pyspark.sql.types import StructType, StructField, DateType, DoubleType

schema_silver = StructType([
    StructField("dt_ref", DateType(), False),
    StructField("vl_selic", DoubleType(), False),
])