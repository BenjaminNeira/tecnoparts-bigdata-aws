# TecnoParts Chile — Arquitectura Big Data en AWS

Proyecto académico de procesamiento distribuido de datos utilizando Amazon Web Services, Apache Spark y Apache ZooKeeper.

## Arquitectura
- Región AWS: us-east-1.
- 2 nodos Spark Master con alta disponibilidad.
- 3 nodos Spark Worker.
- 3 nodos ZooKeeper para coordinación y elección de líder.
- Amazon S3 como Data Lake: raw/, curated/ y results/.
- Apache Spark 3.5.0 y ZooKeeper 3.9.3.

## Dataset
- Semilla: 8255.
- Tamaño generado: 2.484.857.479 bytes.
- Tablas: clientes, productos, ventas, detalle_ventas y navegacion_web.
- Registros de navegación originales: 27.029.056.

## Procesamiento
- Esquemas explícitos con PySpark.
- Reglas de calidad y validaciones de integridad.
- Cruces entre clientes, ventas, detalles y productos.
- Almacenamiento Parquet particionado.
- Segmentación RFM con K-Means.

## Machine Learning
- Clientes analizados: 81.329.
- Configuraciones evaluadas: k=2, k=3, k=4 y k=5.
- Mejor configuración: k=2.
- Coeficiente de silueta: 0,587740.
- Semilla fija: 8255.

## Alta disponibilidad
Se comprobó el failover de master-1 a master-2 mediante ZooKeeper.

- master-1 inicialmente ALIVE.
- master-2 inicialmente STANDBY.
- Elección del nuevo líder registrada el 19/10/2026.
- master-2 asumió el estado ALIVE con tres workers.
- Trabajo Spark posterior al failover completado correctamente.
- master-1 restaurado posteriormente como STANDBY.

## Estructura
- scripts/: generación, ETL, navegación, Machine Learning y pruebas.
- evidencias/: manifiesto del dataset.
- docs/: documentación académica.

## Datos
Los archivos CSV y Parquet no se incluyen en Git por su tamaño. Se almacenan en Amazon S3.

## Uso de inteligencia artificial
Se utilizó ChatGPT como apoyo para la planificación, generación y revisión de comandos, scripts y documentación. Las ejecuciones y validaciones se realizaron en el entorno AWS del proyecto.
