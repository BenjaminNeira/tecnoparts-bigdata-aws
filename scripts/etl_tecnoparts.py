import json, time
from pyspark.sql import SparkSession, functions as F, types as T

B='tecnoparts-bigdata-20261019-bn'
RAW=f's3a://{B}/raw'
CUR=f's3a://{B}/curated'
RES=f's3a://{B}/results'
S=T.StructType
C=lambda name,typ: T.StructField(name,typ,True)
I=T.IntegerType(); L=T.LongType(); D=T.DateType(); TS=T.TimestampType(); ST=T.StringType(); FL=T.FloatType()
schemas={
'clientes':S([C('id_cliente',I),C('rut',ST),C('nombre',ST),C('email',ST),C('genero',ST),C('edad',I),C('segmento',ST),C('region',ST),C('comuna',ST),C('es_empresa',I),C('canal_preferido',ST),C('fecha_registro',D)]),
'productos':S([C('id_producto',I),C('sku',ST),C('nombre',ST),C('categoria',ST),C('subcategoria',ST),C('marca',ST),C('gama',ST),C('precio',I),C('costo',I),C('margen_pct',FL),C('stock',I),C('rating',FL),C('activo',I),C('fecha_alta',D)]),
'ventas':S([C('id_venta',I),C('id_cliente',I),C('fecha_hora',TS),C('fecha',D),C('canal',ST),C('sucursal',ST),C('medio_pago',ST),C('tipo_despacho',ST),C('n_items',I),C('monto_neto',I),C('iva',I),C('monto_total',I),C('estado',ST)]),
'detalle_ventas':S([C('id_detalle',I),C('id_venta',I),C('id_producto',I),C('cantidad',I),C('precio_lista',I),C('descuento_pct',FL),C('precio_unitario',I),C('subtotal',I)]),
'navegacion_web':S([C('id_evento',L),C('id_sesion',I),C('id_cliente',I),C('fecha_hora',TS),C('tipo_evento',ST),C('id_producto',I),C('dispositivo',ST),C('navegador',ST),C('sistema_operativo',ST),C('canal_captacion',ST),C('duracion_seg',I)])}

spark=SparkSession.builder.appName('TecnoParts-ETL-Calidad-Parquet').config('spark.sql.shuffle.partitions','12').config('spark.sql.adaptive.enabled','true').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
metrics=[]
def read(name):
    return spark.read.option('header',True).option('mode','PERMISSIVE').schema(schemas[name]).csv(f'{RAW}/{name}/{name}.csv')
def record(table,rule,before,after):
    metrics.append((table,rule,int(before),int(after),int(before-after)))
    print(f'CALIDAD {table} | {rule}: {before} -> {after} (eliminados {before-after})',flush=True)
def stage(name,df,rules):
    n=df.count()
    for rule,operation in rules:
        new=operation(df)
        m=new.count()
        record(name,rule,n,m)
        df=new; n=m
    return df

def main():
    clientes=stage('clientes',read('clientes'),[
        ('duplicados exactos',lambda d:d.dropDuplicates()),
        ('id_cliente valido',lambda d:d.filter(F.col('id_cliente').isNotNull())),
        ('edad entre 18 y 100',lambda d:d.filter(F.col('edad').between(18,100))),
        ('email presente',lambda d:d.filter(F.col('email').isNotNull() & (F.length(F.trim('email'))>0)))])
    clientes=clientes.withColumn('region',F.initcap(F.trim('region')))
    productos=stage('productos',read('productos'),[
        ('duplicados exactos',lambda d:d.dropDuplicates()),
        ('id y precio validos',lambda d:d.filter(F.col('id_producto').isNotNull() & (F.col('precio')>=0)))])
    ventas=stage('ventas',read('ventas'),[
        ('duplicados exactos',lambda d:d.dropDuplicates()),
        ('fecha valida 2023-2025',lambda d:d.filter(F.col('fecha').between('2023-01-01','2025-12-31'))),
        ('cliente existente JOIN 1',lambda d:d.join(F.broadcast(clientes.select('id_cliente').distinct()),'id_cliente','left_semi'))])
    detalle=stage('detalle_ventas',read('detalle_ventas'),[
        ('duplicados exactos',lambda d:d.dropDuplicates()),
        ('cantidad positiva',lambda d:d.filter(F.col('cantidad')>0)),
        ('precio unitario presente',lambda d:d.filter(F.col('precio_unitario').isNotNull())),
        ('venta existente JOIN 2',lambda d:d.join(ventas.select('id_venta').distinct(),'id_venta','left_semi')),
        ('producto existente JOIN 3',lambda d:d.join(F.broadcast(productos.select('id_producto').distinct()),'id_producto','left_semi'))])
    ventas=ventas.withColumn('anio',F.year('fecha')).withColumn('mes',F.month('fecha'))
    print('ESCRIBIENDO clientes',flush=True)
    clientes.coalesce(2).write.mode('overwrite').parquet(f'{CUR}/clientes')
    print('ESCRIBIENDO productos',flush=True)
    productos.coalesce(1).write.mode('overwrite').parquet(f'{CUR}/productos')
    print('ESCRIBIENDO ventas particionadas',flush=True)
    ventas.repartition(6,'anio','mes').write.mode('overwrite').partitionBy('anio','mes').parquet(f'{CUR}/ventas')
    print('ESCRIBIENDO detalle particionado por categoria',flush=True)
    det_cat=detalle.join(F.broadcast(productos.select('id_producto','categoria')),'id_producto','inner')
    det_cat.repartition(6,'categoria').write.mode('overwrite').partitionBy('categoria').parquet(f'{CUR}/detalle_ventas')
    print('CALCULANDO INSIGHTS',flush=True)
    fact=ventas.select('id_venta','id_cliente','fecha','monto_total','canal')
    fact.groupBy('canal').agg(F.count('*').alias('ventas'),F.sum('monto_total').alias('ingresos')).orderBy(F.desc('ingresos')).coalesce(1).write.mode('overwrite').option('header',True).csv(f'{RES}/ventas_por_canal')
    rfm=fact.groupBy('id_cliente').agg(F.datediff(F.lit('2026-01-01').cast('date'),F.max('fecha')).alias('recencia_dias'),F.countDistinct('id_venta').alias('frecuencia'),F.sum('monto_total').alias('monetario'))
    rfm.write.mode('overwrite').parquet(f'{RES}/rfm_base')
    print('ESCRIBIENDO METRICAS DE CALIDAD',flush=True)
    metric_schema=S([C('tabla',ST),C('regla',ST),C('filas_antes',L),C('filas_despues',L),C('filas_eliminadas',L)])
    spark.createDataFrame(metrics,metric_schema).coalesce(1).write.mode('overwrite').option('header',True).csv(f'{RES}/calidad_datos')
    print('ETL_COMPLETADO',flush=True)

try:
    main()
finally:
    spark.stop()
