from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
hc = HiveContext(sc)

### Inicio del Script ###
# Leemos los csv desde HDFS y cargamos en dataframes
df_rental = spark.read.option("header", "true").option("sep", ",").csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")
df_georef = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/georef.csv")

# Dropeamos columnas que no utilizaremos
df_rental = df_rental.drop('location.country', 'location.latitude', 'location.longitude', 'vehicle.type')
df_georef = df_georef.drop('State FIPS Code')

# Normalizamos nombres de columnas
# Importante mantener linea vacia debajo de cada bucle for
for column in df_rental.columns:
    df_rental = df_rental.withColumnRenamed(column, column.replace('.','_'))

df_rental = df_rental.withColumnRenamed("location_city","city").withColumnRenamed("location_state","state_name")

for column in df_rental.columns[-3:]:
    df_rental = df_rental.withColumnRenamed(column, column.split('_')[-1])

for column in df_georef.columns:
    df_georef = df_georef.withColumnRenamed(column, column.replace('.','_').replace(' ','_'))

for column in df_georef.columns:
    df_georef = df_georef.withColumnRenamed(column,'_'.join(column.split('_')[-2:]))

df_georef = df_georef.withColumnRenamed("Year", "year_georef")

# Redondeamos los float de 'rating' y castear a int
# Vuelvo a cargar sql.functinos porque levanta error
from pyspark.sql.functions import *
df_rental = df_rental.filter(column("rating").isNotNull())

# Eliminamos 'Texas'
df_rental = df_rental.filter("state_name != 'TX'")

# Joineamos las tablas en 'state_name'
df_rental = df_rental.join( df_georef, df_rental.state_name==df_georef.state_abbreviation, 'left')

# Casteamos las variables no-string
cast_cols = ['renterTripsTaken', 'reviewCount', 'owner_id', 'rate_daily', 'year', 'year_georef', 'Code_State', 'GNIS_Code']
for column in cast_cols:
    df_rental = df_rental.withColumn( column, col(column).cast('int'))

# Redondeamos los float de 'rating' y castear a int
df_rental = df_rental.withColumn( 'rating', round('rating').cast('integer') )

# Mayusculas por minusculas en 'fuelType'
df_rental = df_rental.withColumn('fuelType', lower('fuelType') )

# Dropeo columna 'Geo_Point' y 'Geo_Shape'
#  --AVERIGUAR COMO TRATAR CON LISTAS GEO-SHAPE
#  EL OBJETO DE LISTAS SE CARGA INCORRECCTAMENTE EN HIVE--
df_rental = df_rental.drop('Geo_Point', 'Geo_Shape')

# Creamos vistas con la data filtrada
df_rental.createOrReplaceTempView("df_rental_vista")

# Insertamos DFs filtrados en tablas de Hive
hc.sql("insert into car_rental_db.car_rental_analytics select * from df_rental_vista;")

### Fin del Script ###