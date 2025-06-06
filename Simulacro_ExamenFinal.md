# Simulacro Examen Final

## Pregunta 1

En este ejercicio, vamos a utilizar datos desestrucutados sobre tweets de temática financiera para convertirlos en datos estructurados, analizarlos y volcar ciertos datos sobre un sink de Streaming (topic de Kafka). El comando o código (se especificará si deberá responder con la API de Dataframe o con sentencias spark.sql) utilizado en cada paso debe ser incluído en formato texto en el campo de respuesta de la pregunta de Blackboard como solución final:

---

> **Paso 1:** Descargar el fichero de datos (*No requiere incluir los comandos en la solución final*) y explora el contenido de dicho fichero (*por ejemplo, con el comando head*).
>

**NOTA:** Aunque los datos cumplen el siguiente formato: <texto-tweet> *AT* <timestamp-tweet> *FROM* <user-twitter> *WITH_VERIFICATION* [True|False], se deberán tratar incialmente como datos desestructurados

```bash
wget -P /home/bigdata/Descargas/ [https://gist.githubusercontent.com/mafernandez-stratio/904c6085de256a5227c739ebff54ad12/raw/2a979108404a3f4e24f6437864da6bdb6ae5a562/tweets.txt](https://gist.githubusercontent.com/mafernandez-stratio/904c6085de256a5227c739ebff54ad12/raw/2a979108404a3f4e24f6437864da6bdb6ae5a562/tweets.txt)
```

---

> **Paso 2:** Arranca HDFS y copia el fichero tweets.txt a la ruta /data/tweets/ de HDFS
>

```bash
cd hadoop
sbin/start-dfs.sh
```

```bash
# Crear el directorio en HDFS si no existe
/home/bigdata/hadoop-3.3.6/bin/hdfs dfs -mkdir -p /data/tweets/
# Copiar el fichero tweets.txt a HDFS
/home/bigdata/hadoop-3.3.6/bin/hdfs dfs -put /home/bigdata/Descargas/tweets.txt /data/tweets/
```

---

> **Paso 3:** Arranca la shell de Spark con la dependencia del conector de Kafka (--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3) y genera un RDD que cargue las líneas de los datos sobre tweets albergados en HDFS
>

```bash
cd spark-3.3.3-bin-hadoop3/
bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3
```

```scala
val tweetsRDD = spark.sparkContext.textFile("hdfs://localhost:9000/data/tweets/tweets.txt")
```

---

> **Paso 4:** Convierte este RDD en un Dataframe con la siguiente estructura -> text: String, timestamp: Long, profile: String, verified: Boolean
>

**NOTA:** Si tenemos la línea: "Mi contenido tweet *AT* 1531949606 *FROM* BigDataUser *WITH_VERIFICATION* True", nuestro Dataframe contendrá una fila donde -> text="Mi contenido tweet", timestamp=1531949606, profile="BigDataUser" y verified=true

```scala
case class Tweet(text: String, timestamp: Long, profile: String, verified: Boolean)
```

```scala
val tweetsDF = tweetsRDD.map ({  linea =>
	val partAT = linea.split(" _AT_ ")
	val text = partAT(0)
	val partFROM = partAT(1).split(" _FROM_ ")
	val timestamp = partFROM(0).toLong
	val partWITH = partFROM(1).split(" _WITH_VERIFICATION_ ")
	val user = partWITH(0)
	val verification = partWITH(1).toBoolean
	Tweet(text, timestamp, user, verification)
}).toDF()
```

---

> **Paso 5:** Create una TempView a partir de este Dataframe que se llame "tweets". Muestra el listado de tablas y una descripción de la vista creada en el catálogo de SparkSQL
>

```scala
tweetsDF.createOrReplaceTempView("tweets")
```

```scala
spark.catalog.listTables().show(false)
```

```scala
// Usando API DataFrame
spark.catalog.listColumns("tweets").show(false)
// Usando spark.sql
spark.sql("DESCRIBE tweets").show(false)
```

---

> **Paso 6:** Responde con una sentencia SQL: ¿cuántos usarios distintos hay en la vista tweets?
>

```scala
spark.sql("SELECT COUNT(DISTINCT profile) FROM tweets").show(false)
```

---

> **Paso 7:** Responde con una sentencia SQL: ¿cuáles son los 5 usuarios con más tweets?
>

```scala
spark.sql("SELECT profile, COUNT(*) AS tweet_count FROM tweets GROUP BY profile ORDER BY tweet_count DESC LIMIT 5").show()
```

---

> **Paso 8:** Responde con una sentencia SQL: ¿cuáles son los 5 usuarios con un mayor rango de tiempo entre su tweet más antiguo y más moderno?
>

**NOTA:** Si un usuario tiene 3 tweets en los datos con estos timestamps: 1532111000, 1533111000 y 1534111000; entonces su rango de tiempo será 1534111000 - 1532111000 = 2000000

```scala
spark.sql("SELECT profile, MAX(time) - MIN (time) AS time_range FROM tweets GROUP BY profile ORDER BY time_range DESC LIMIT 5").show()
```

---

> **Paso 9:** Responde con una sentencia SQL: Create una tabla llamada verified_users en formato parquet que almacene sus datos en la ruta /data/verified/ de HDFS y que contenga 2 columnas: key (concatenación de las columnas profile y timestamp de la vista tweets) y value (columna text de la vista tweets)
>

```scala
spark.sql("CREATE TABLE verified_users USING PARQUET
					LOCATION 'hdfs://localhost:9000/data/verified/'
					AS SELECT
						CONCAT(profile, '_', CAST(timestamp AS STRING)) AS key,
						text AS value
					FROM tweets
					WHERE verified = true")
```

---

> **Paso 10:** Inicia un servicio de Kafka (es decir, levantar un Broker de Kafka) y, posteriormente, una consola consumidor de Kafka del topic tweets, que pertenezca al consumer group grupo1, que imprima la clave de cada mensaje y que utilice el caracter : como separador entre la clave y el valor
>

```bash
confluent local services kafka start
```

```bash
cd confluent
bin/kafka-topics --create --topic tweets --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

```bash
kafka-console-producer \
  --broker-list localhost:9092 \
  --topic tweets \
  --property "parse.key=true" \
  --property "key.separator=:"

# Desde aquí se pueden mandar cosas con el formato usuario_timestamp:mensaje
```

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tweets \
  --group grupo1 \
  --property print.key=true \
  --property key.separator=":"

# Aquí se recibirá el mensaje
```

---

> Paso 11: Lee los datos de la tabla verified_users como una fuente de Streaming y vuelca su contenido en el topic tweets de Kafka
>

```bash
val schema = spark.read.parquet("hdfs://localhost:9000/data/verified/").schema

val streamingDF = spark.readStream.schema(schema).format("parquet").load("hdfs://localhost:9000/data/verified/")

val query = streamingDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "tweets").option("checkpointLocation", "/tmp/checkpoints/verified_to_kafka").start()
```

---

> Paso 12: Recupera el objeto StreamingQuery de la query lanzada en el paso anterior para conseguir su estado, su último progreso y para parar dicha StreamingQuery
>

```bash
val query = spark.streams.active(0)

query.status

println(query.lastProgress.prettyJson)

query.stop()
```

## Pregunta 2

En este ejercicio, vamos a utilizar datos sobre las carreras de los taxis amarillos de Nueva York en Enero de 2024 (se incluye un documento PDF con el diccionario de los metadatos). Además, se usará otro fichero de datos con información sobre los barrios de NYC. El comando o código utilizado (TODOS los PASOS realizados con Spark en este ejercicio, deben ser resueltos con la API de Dataframe, es decir, no se puede utilizar el método spark.sql para lanzar sentencias SQL) en cada paso debe ser incluído en formato texto en el campo de respuesta de la pregunta de Blackboard como solución final:

---

> **Paso 1:** Descargar ficheros de datos (No requiere incluir los comandos en la solución final) y explora el contenido del fichero csv (por ejemplo, con el comando head)
>

```bash
wget -P /home/bigdata/Descargas/ [https://gist.githubusercontent.com/mafernandez-stratio/e4a3d3f0c4790989201b2c4af3daa1a1/raw/a983fcf3d1f7beb274c1c792add8a00bb968a5f2/taxi_zones.json](https://gist.githubusercontent.com/mafernandez-stratio/e4a3d3f0c4790989201b2c4af3daa1a1/raw/a983fcf3d1f7beb274c1c792add8a00bb968a5f2/taxi_zones.json)

wget -P /home/bigdata/Descargas/ https://gist.githubusercontent.com/mafernandez-stratio/ffc511cfcc5b1c4b260523127689e24b/raw/6bae4169c89c49adb6ec6c09426af2226290d493/yellow_tripdata_2024-01.csv
```

---

> Paso 2: Arranca HDFS y copia el fichero taxi_zones.json a la ruta /data/zones/ de HDFS y el fichero yellow_tripdata_2024-01.csv a la ruta /data/taxis/
>

```bash
/home/bigdata/hadoop-3.3.6/bin/hdfs dfs -mkdir -p /data/taxis/
/home/bigdata/hadoop-3.3.6/bin/hdfs dfs -mkdir -p /data/zones/

hdfs dfs -put yellow_tripdata_2024-01.csv  /data/taxis
hdfs dfs -put taxi_zones.json  /data/zones
```

---

> **Paso 3:** Arranca la shell de Spark y genera un Dataframe que cargue los datos sobre taxis albergados en HDFS (*incluye las opciones del datasource de csv que estimes oportunas tras haber realizado la exploración del fichero en el paso anterior*):
>

```scala
val dfTaxis = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ":").option("quote", "'").csv("hdfs://localhost:9000/data/taxis/yellow_tripdata_2024-01.csv")

dfTaxis.show(5)
dfTaxis.printSchema()
```

---

> **Paso 4:** ¿Cuántos registros tienen un total_amount menor o igual a 0?
>

```scala
import org.apache.spark.sql.functions._

val registrosInvalidos = dfTaxis.filter($"total_amount" <= 0)
val cantidad = registrosInvalidos.count()
println(s"Número de registros con total_amount <= 0: $cantidad")
```

---

> **Paso 5:** Genera una UDF con el nombre calculateTime que obtenga la diferencia en segundos entre 2 timestamps expresados en nanosegundos. Estos timestamps realmente se tratan en Spark como un Long, por lo que la UDF deberá recibir dos variables de tipo Long (ambos timestamps en nanonsegundos) y devolver un Long (diferencia en segundos):
>

```scala
import org.apache.spark.sql.functions.udf

val calculateTime = udf((start: Long, end: Long) => {
  (end - start) / 1000000000L
})
```

---

> **Paso 6:** Genera un Dataframe partiendo del Dataframe del paso 3 con las siguientes operaciones:
>

```scala
val processedTaxiDF = taxiDF
	// Nueva columna ride_time resultado de ejecutar la UDF calculateTimeUDF sbre las columnas tpep_pickup_datetime y tpep_dropoff_datetime
  .withColumn("ride_time", calculateTime(col("tpep_pickup_datetime").cast(LongType), col("tpep_dropoff_datetime").cast(LongType)))
  // Nueva columna Airport_charge que tenga el valor 0 si el valor de la columna Airport_fee es nulo o el propio valor de Airport_fee en caso contrario
  .withColumn("Airport_charge", when(col("Airport_fee").isNull, 0).otherwise(col("Airport_fee")))
  // Filtrar las filas que tengan un total_amount mayor que 0, es decir, descartar aquellas filas cuyo total_amount sea 0 o negativo
  .filter(col("total_amount") > 0)
  // Elimina la columna LocationID del Dataframe resultante
  .drop("LocationID")

processedTaxiDF.printSchema()
processedTaxiDF.show(5, false)
```

---

> **Paso 7:** Genera un nuevo Dataframe desde 0 que cargue los datos sobre zonas de NYC albergados en HDFS:
>

```scala
val dfZones = spark.read.option("multiline", "true").json("hdfs://localhost:9000/data/zones/taxi_zones.json")
```

---

> **Paso 8:** Genera un Dataframe partiendo del Dataframe del paso 6 con las siguientes operaciones:
>

```scala
val joinedTaxiDF = processedTaxiDF
	// Nueva columna PUborough (correspondiente a la columna borough de zonas) generada de un inner join entre las columnas PULocationID de taxis y LocationID de zonas
  .join(zonesDF.select(col("LocationID"), col("borough").alias("PUborough")), processedTaxiDF("PULocationID") === zonesDF("LocationID"), "inner")
  // Nueva columna DOborough (correspondiente a la columna borough de zonas) generada de un inner join entre las columnas DOLocationID de taxis y LocationID de zonas
  .join(zonesDF.select(col("LocationID"), col("borough").alias("DOborough")), processedTaxiDF("DOLocationID") === zonesDF("LocationID"), "inner")
  // Elimina la columna LocationID del Dataframe resultante
  .drop("LocationID") // Drop the LocationID columns after joins

joinedTaxiDF.printSchema()
joinedTaxiDF.show(5, false)
```

> **Paso 9:** ¿Cuál es la suma total y cuál es el máximo de la columna total_amount? ¿Cuántas carreras duraron más de 1 hora?
>

```scala
joinedTaxiDF.agg(sum("total_amount").alias("total_amount_sum"), max("total_amount").alias("total_amount_max")).show()
joinedTaxiDF.filter(col("ride_time") > 3600).count() // 3600 seconds = 1 hour
```

---

> **Paso 10:** Calcula la media de trip_distance por PUborough y payment_type ordenados por PUborough y payment_type
>

```scala
joinedTaxiDF.groupBy("PUborough", "payment_type").agg(avg("trip_distance").alias("avg_trip_distance")).orderBy("PUborough", "payment_type").show()
```

---

> **Paso 11:** Calcula cuántos valores nulos hay por columna
>

```scala
val nullCounts = joinedTaxiDF.columns.map(c => sum(col(c).isNull.cast("int")).alias(c + "_nulls"))
joinedTaxiDF.agg(nullCounts.head, nullCounts.tail:_*).show()
```

---

> Paso 12: Obten una lista de nombres de columnas del Dataframe del paso 8 que sean de tipo numéricas (integer, double y long) y calcula la correlación entre estas columnas y la columna total_amount
>

```scala
val numericCols = joinedTaxiDF.dtypes.filter{case(c, t) => t.equalsIgnoreCase("IntegerType") || t.equalsIgnoreCase("DoubleType") || t.equalsIgnoreCase("LongType")}.map(_._1)

numericCols.foreach { colName =>
  if (colName != "total_amount") { // Exclude correlating with itself
    joinedTaxiDF.select(corr(colName, "total_amount").alias(s"corr_${colName}_total_amount")).show()
  }
}
```

---

> **Paso 13:** Genera un Dataframe que realice cuatro transformaciones con SparK MLlib sobre el Dataframe del paso 8 como paso previo a generar una Regresión Lineal sobre la columna total_amount basada en las columnas trip_distance, tolls_amount, Airport_charge, ride_time, PUborough (*NOTA: PUborough incialmente es una columna de tipo String*)
>

```scala
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline

// StringIndexer for PUborough
val indexer = new StringIndexer()
  .setInputCol("PUborough")
  .setOutputCol("PUborough_indexed")

// OneHotEncoder for PUborough_indexed (optional but good practice for nominal categorical features)
// Although for Linear Regression, StringIndexer might be sufficient, OneHotEncoder is generally preferred for nominal categories.
// For simplicity, we'll use just StringIndexer output as a numeric feature here, as per the typical example flow.
// If you wanted OHE:
// val encoder = new OneHotEncoder().setInputCol("PUborough_indexed").setOutputCol("PUborough_encoded")

// VectorAssembler to combine features
val assembler = new VectorAssembler()
  .setInputCols(Array("trip_distance", "tolls_amount", "Airport_charge", "ride_time", "PUborough_indexed"))
  .setOutputCol("features")

// Create a pipeline to combine transformations
val pipeline = new Pipeline().setStages(Array(indexer, assembler))

// Fit the pipeline to the data
val pipelineModel = pipeline.fit(joinedTaxiDF)

// Transform the data
val transformedDF = pipelineModel.transform(joinedTaxiDF)

transformedDF.select("total_amount", "trip_distance", "tolls_amount", "Airport_charge", "ride_time", "PUborough", "PUborough_indexed", "features").show(5, false)
```

---

> Paso 14: Genera un Dataframe que contenga solo las columnas total_amount y la columna del vector de características calculado en el paso anterior. A partir de este Dataframe, genera un Dataframe de datos de entrenamiento (70%) y un Dataframe de datos de testing (30%).
>

```scala
val model_df = transformedDF.select("features", "total_amount")

val Array(train_df, test_df) = model_df.randomSplit(Array(0.7, 0.3), seed = 42) // Using a seed for reproducibility

train_df.show(5, false)
test_df.show(5, false)
```

> Paso 15: ¿Cuántas filas tiene cada Dataframe generado en el paso anterior? Lanza también un comando sobre cada Dataframe para obtener count, mean, stddev, min y max de la columna total_amount
>

```scala
println(s"Train DataFrame rows: ${train_df.count()}")
println(s"Test DataFrame rows: ${test_df.count()}")

println("Train DataFrame - total_amount statistics:")
train_df.select(
  count("total_amount").alias("count"),
  mean("total_amount").alias("mean"),
  stddev("total_amount").alias("stddev"),
  min("total_amount").alias("min"),
  max("total_amount").alias("max")
).show()

println("Test DataFrame - total_amount statistics:")
test_df.select(
  count("total_amount").alias("count"),
  mean("total_amount").alias("mean"),
  stddev("total_amount").alias("stddev"),
  min("total_amount").alias("min"),
  max("total_amount").alias("max")
).show()
```

---

> Paso 16: Genera un modelo y un Dataframe con predicciones de una regresión lineal a partir del Dataframe de entrenamiento. Obten el coeficiente de determinación (R2) y explica si el modelo de regresión lineal es suficientemente preciso en función de dicho valor.
>

```scala
import org.apache.spark.ml.regression.LinearRegression

val lin_Reg = new LinearRegression().setLabelCol("total_amount").setFeaturesCol("features")

val lr_model = lin_Reg.fit(train_df)

val training_predictions = lr_model.transform(train_df)
training_predictions.show(10, false)

val r2_train = lr_model.summary.r2
println(s"Coefficient of determination (R2) on training data: $r2_train")

// Explanation of R2
if (r2_train >= 0.75) {
  println("El modelo de regresión lineal es bastante preciso en el conjunto de entrenamiento, ya que el valor de R2 es alto, indicando que una gran proporción de la variabilidad en la variable dependiente es explicada por las características del modelo.")
} else if (r2_train >= 0.5) {
  println("El modelo de regresión lineal tiene una precisión moderada en el conjunto de entrenamiento. Explica una parte significativa, pero no la mayoría, de la variabilidad en la variable dependiente.")
} else {
  println("El modelo de regresión lineal no es muy preciso en el conjunto de entrenamiento, ya que el valor de R2 es bajo, sugiriendo que las características del modelo no explican bien la variabilidad de la variable dependiente.")
}
```

---

> Paso 17: Evalúa el modelo obteniendo un resumen (LinearRegressionSummary) de acuerdo a los datos de testing, obten el coeficiente de determinación (R2) y explica si el modelo de regresión lineal es suficientemente preciso en función de dicho valor.
>

```scala
val test_results = lr_model.evaluate(test_df)
test_results.predictions.show(10, false)

val r2_test = test_results.r2
println(s"Coefficient of determination (R2) on test data: $r2_test")

// Explanation of R2
if (r2_test >= 0.75) {
  println("El modelo de regresión lineal es bastante preciso en el conjunto de prueba, ya que el valor de R2 es alto, indicando una buena capacidad de generalización y que el modelo explica bien la variabilidad de la variable dependiente en datos no vistos.")
} else if (r2_test >= 0.5) {
  println("El modelo de regresión lineal tiene una precisión moderada en el conjunto de prueba. Aunque explica una parte significativa de la variabilidad, podría haber margen para mejorar la generalización del modelo.")
} else {
  println("El modelo de regresión lineal no es muy preciso en el conjunto de prueba, ya que el valor de R2 es bajo, sugiriendo que el modelo no se generaliza bien a datos no vistos y las características no explican adecuadamente la variabilidad de la variable dependiente.")
}

println(s"Mean Squared Error (MSE) on test data: ${test_results.meanSquaredError}")
println(s"Root Mean Squared Error (RMSE) on test data: ${test_results.rootMeanSquaredError}")
```
