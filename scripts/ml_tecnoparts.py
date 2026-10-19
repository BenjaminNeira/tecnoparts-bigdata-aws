from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark import StorageLevel

B='tecnoparts-bigdata-20261019-bn'
RES=f's3a://{B}/results'
spark=(SparkSession.builder.appName('TecnoParts-RFM-KMeans-8255')
       .config('spark.sql.shuffle.partitions','12')
       .config('spark.sql.adaptive.enabled','true').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
try:
    raw=spark.read.parquet(f'{RES}/rfm_base')
    df=raw.filter((F.col('recencia_dias')>=0)&(F.col('frecuencia')>0)&(F.col('monetario')>=0)).dropna()
    for c in ['recencia_dias','frecuencia','monetario']:
        df=df.withColumn('log_'+c,F.log1p(F.col(c).cast('double')))
    assembler=VectorAssembler(inputCols=['log_recencia_dias','log_frecuencia','log_monetario'],outputCol='features_raw')
    vectorized=assembler.transform(df)
    scaler=StandardScaler(inputCol='features_raw',outputCol='features',withStd=True,withMean=True).fit(vectorized)
    ready=scaler.transform(vectorized).select('id_cliente','recencia_dias','frecuencia','monetario','features').persist(StorageLevel.MEMORY_AND_DISK)
    print('RFM_CLIENTES',ready.count(),flush=True)
    evaluator=ClusteringEvaluator(featuresCol='features',predictionCol='prediction',metricName='silhouette',distanceMeasure='squaredEuclidean')
    scores=[]; best_k=None; best_score=-2.0; best_model=None
    for k in [2,3,4,5]:
        model=KMeans(k=k,seed=8255,featuresCol='features',predictionCol='prediction',maxIter=20).fit(ready)
        pred=model.transform(ready)
        score=float(evaluator.evaluate(pred))
        scores.append((k,score,float(model.summary.trainingCost)))
        print(f'KMEANS k={k} silhouette={score:.6f} costo={model.summary.trainingCost:.2f}',flush=True)
        if score>best_score:
            best_k=k;best_score=score;best_model=model
    spark.createDataFrame(scores,['k','silueta','costo_entrenamiento']).coalesce(1).write.mode('overwrite').option('header',True).csv(f'{RES}/kmeans_comparacion')
    predictions=best_model.transform(ready).select('id_cliente','recencia_dias','frecuencia','monetario',F.col('prediction').alias('segmento'))
    predictions.write.mode('overwrite').parquet(f'{RES}/kmeans_segmentos')
    summary=(predictions.groupBy('segmento').agg(F.count('*').alias('clientes'),F.round(F.avg('recencia_dias'),2).alias('recencia_promedio_dias'),F.round(F.avg('frecuencia'),2).alias('frecuencia_promedio'),F.round(F.avg('monetario'),2).alias('gasto_promedio_clp'),F.sum('monetario').alias('ingresos_totales_clp')).orderBy('segmento'))
    summary.coalesce(1).write.mode('overwrite').option('header',True).csv(f'{RES}/kmeans_resumen')
    print('MEJOR_K',best_k,'SILUETA',round(best_score,6),flush=True)
    summary.show(truncate=False)
    print('ML_COMPLETADO',flush=True)
    ready.unpersist()
finally:
    spark.stop()
