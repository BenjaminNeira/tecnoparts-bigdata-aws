from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TecnoParts-Prueba-S3")
    .getOrCreate()
)

ruta = "s3a://tecnoparts-bigdata-20261019-bn/raw/productos/productos.csv"

df = spark.read.option("header", True).csv(ruta)

print("=== PRUEBA S3 ===")
print("Total productos:", df.count())
df.select("id_producto", "nombre", "categoria").show(5, truncate=False)

spark.stop()
