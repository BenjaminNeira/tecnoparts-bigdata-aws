"""ETL de navegacion web TecnoParts: esquema, limpieza y Parquet particionado."""
from pyspark.sql import SparkSession, functions as F, types as T

BUCKET = 'tecnoparts-bigdata-20261019-bn'
SRC = f's3a://{BUCKET}/raw/navegacion_web/navegacion_web.csv'
DST = f's3a://{BUCKET}/curated/navegacion_web'
MET = f's3a://{BUCKET}/results/calidad_navegacion'
SUM = f's3a://{BUCKET}/results/navegacion_por_evento'

schema = T.StructType([
    T.StructField('id_evento', T.LongType(), True),
    T.StructField('id_sesion', T.IntegerType(), True),
    T.StructField('id_cliente', T.IntegerType(), True),
    T.StructField('fecha_hora', T.TimestampType(), True),
    T.StructField('tipo_evento', T.StringType(), True),
    T.StructField('id_producto', T.IntegerType(), True),
    T.StructField('dispositivo', T.StringType(), True),
    T.StructField('navegador', T.StringType(), True),
    T.StructField('sistema_operativo', T.StringType(), True),
    T.StructField('canal_captacion', T.StringType(), True),
    T.StructField('duracion_seg', T.IntegerType(), True),
])

spark = (SparkSession.builder.appName('TecnoParts-Navegacion-ETL')
         .config('spark.sql.shuffle.partitions', '12')
         .config('spark.sql.adaptive.enabled', 'true')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')
try:
    raw = spark.read.option('header', True).option('mode', 'PERMISSIVE').schema(schema).csv(SRC)
    # Una sola pasada de lectura para contar las principales anomalias.
    stats = raw.agg(
        F.count('*').alias('total'),
        F.sum(F.when(F.col('id_evento').isNull(), 1).otherwise(0)).alias('id_nulo'),
        F.sum(F.when(F.col('fecha_hora').isNull(), 1).otherwise(0)).alias('fecha_nula'),
        F.sum(F.when(F.col('duracion_seg') < 0, 1).otherwise(0)).alias('duracion_negativa'),
    ).first().asDict()
    print('NAVEGACION_CALIDAD_INICIAL', stats, flush=True)

    clean = (raw.filter(F.col('id_evento').isNotNull())
             .filter(F.col('fecha_hora').isNotNull())
             .filter(F.col('duracion_seg').isNull() | (F.col('duracion_seg') >= 0))
             .withColumn('tipo_evento', F.lower(F.trim('tipo_evento')))
             .withColumn('anio', F.year('fecha_hora'))
             .withColumn('mes', F.month('fecha_hora')))
    # No aplicar dropDuplicates global a 27 M de filas en este cluster pequeno.
    # Los duplicados del origen se documentan mediante el manifiesto del generador.
    print('NAVEGACION_ESCRIBIENDO_PARQUET', flush=True)
    clean.repartition(6, 'anio', 'mes').write.mode('overwrite').partitionBy('anio', 'mes').parquet(DST)
    print('NAVEGACION_PARQUET_OK', flush=True)

    # Leer el parquet escrito permite validar el resultado sin recalcular el CSV original.
    curated = spark.read.parquet(DST)
    counts = curated.groupBy('tipo_evento').count().orderBy(F.desc('count'))
    counts.coalesce(1).write.mode('overwrite').option('header', True).csv(SUM)
    after = curated.count()
    metrics = [
        ('filas_originales', int(stats['total'])),
        ('id_evento_nulo', int(stats['id_nulo'] or 0)),
        ('fecha_hora_nula', int(stats['fecha_nula'] or 0)),
        ('duracion_negativa', int(stats['duracion_negativa'] or 0)),
        ('filas_curadas', int(after)),
        ('filas_descartadas_total', int(stats['total']) - int(after)),
    ]
    spark.createDataFrame(metrics, 'metrica string, cantidad long').coalesce(1).write.mode('overwrite').option('header', True).csv(MET)
    print('NAVEGACION_FILAS_ORIGINALES', stats['total'], flush=True)
    print('NAVEGACION_FILAS_CURADAS', after, flush=True)
    print('NAVEGACION_ETL_COMPLETADO', flush=True)
finally:
    spark.stop()
