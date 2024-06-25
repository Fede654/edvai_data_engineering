from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
hc = HiveContext(sc)

### Inicio del Script ###
# Leemos los csv desde HDFS y cargamos en dataframes
df1 = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
df2 = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/2022-informe-ministerio.csv")
df_aerop = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")

# Unimos las tablas de vuelos
df_vuelos = df1.union(df2).distinct()

# Dropeamos columnas que no utilizaremos
df_vuelos = df_vuelos.drop('Calidad dato')
df_aerop = df_aerop.drop("inhab", "fir")

# Normalizamos nombres de columnas
df_vuelos = df_vuelos.withColumnRenamed("Hora UTC","horaUTC").withColumnRenamed("Clase de Vuelo (todos los vuelos)","clase_de_vuelo").withColumnRenamed("Clasificación Vuelo","clasificacion_de_vuelo").withColumnRenamed("Tipo de Movimiento","tipo_de_movimiento").withColumnRenamed("Aeropuerto","aeropuerto").withColumnRenamed("Origen / Destino","origen_destino").withColumnRenamed("Aerolinea Nombre","aerolinea_nombre").withColumnRenamed("Aeronave","aeronave").withColumnRenamed("Pasajeros","pasajeros")
df_aerop = df_aerop.withColumnRenamed("local","aeropuerto").withColumnRenamed("oaci","oac")

# Casteamos las variables no-string
df_vuelos = df_vuelos.withColumn("fecha", to_date("fecha", "dd/MM/yyyy").alias("fecha")).withColumn("pasajeros", col('pasajeros').cast('int'))
df_aerop = df_aerop.withColumn("elev", col('elev').cast('float')).withColumn("distancia_ref", col('distancia_ref').cast('float'))

# Filtramos por fecha
df_vuelos = df_vuelos.filter("fecha >= '2021-01-01' AND fecha <= '2022-06-30'")

# Dropeamos los vuelos internacionales
df_vuelos = df_vuelos.filter("clasificacion_de_vuelo != 'Internacional'")

# Tratamos los nulos
df_vuelos = df_vuelos.withColumn('pasajeros', when(col('pasajeros').isNull(), 0 ).otherwise(col('pasajeros')))
df_aerop = df_aerop.withColumn('distancia_ref', when(col('distancia_ref').isNull(), 0. ).otherwise(col('distancia_ref')))

# Creamos vistas con la data filtrada
df_vuelos.createOrReplaceTempView("df_vuelos_vista")
df_aerop.createOrReplaceTempView("df_aerop_vista")

# Insertamos DFs filtrados en tablas de Hive
hc.sql("insert into aviacion_civil.aeropuerto_tabla select * from df_vuelos_vista;")
hc.sql("insert into aviacion_civil.aeropuerto_detalles_tabla select * from df_aerop_vista;")

### Fin del Script ###