//nc -lk 9999

val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()
val stream1 = wordCounts.writeStream.outputMode("complete").format("console").start()

val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.withColumn("timestamp", current_timestamp()).select(col("timestamp"), upper(col("value")))
val stream2 = wordCounts.writeStream.outputMode("append").format("console").start()

val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()
val stream3 = wordCounts.writeStream.outputMode("update").format("console").start()

import scala.concurrent._
implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()
Future(wordCounts.writeStream.outputMode("complete").format("memory").queryName("streaming1").start())
val query = spark.streams.active.head
spark.sql("SHOW TABLES").show(500, false)
spark.sql("SELECT * FROM streaming1").show(500, false)
query.isActive
query.id
query.runId
query.status
query.lastProgress
query.recentProgress
query.stop
query.status


// Ejercicio 1: Utiliza foreachBatch para escribir los datos del DataFrame wordCounts en csv y en json en /home/bigdata/streaming1csv/ y /home/bigdata/streaming1json/
// Como salida, debe haber un único fichero de cada formato y debe sobreescribirse por cada ventana
import org.apache.spark.sql._
val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()
// Solución:
wordCounts.writeStream.outputMode("complete").foreachBatch{ (batchDF: DataFrame, batchId: Long) =>
  batchDF.coalesce(1).write.mode("overwrite").csv("/home/bigdata/streaming1csv")
  batchDF.coalesce(1).write.mode("overwrite").json("/home/bigdata/streaming1json")
}.start()

// Ejercicio 2: Agrupa por ventanas de 30 segundos el DataFrame words. Ten en cuenta que se debe tener una columna con el timestamp del evento
val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
// Solución:
val windowedCounts = words.withColumn("timestamp", current_timestamp()).groupBy(window($"timestamp", "30 seconds")).count()
val query = windowedCounts.writeStream.outputMode("complete").format("console").option("truncate", false).start()

import org.apache.spark.sql.streaming._
val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()
wordCounts.writeStream.outputMode("complete").format("console").trigger(Trigger.ProcessingTime("30 seconds")).start()

//Ejercicio 3: Repite la query anterior con trigger Once y explica lo que ocurre
import org.apache.spark.sql.streaming._
val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()
// Solución:
wordCounts.writeStream.outputMode("complete").format("console").trigger(Trigger.Once()).start()
// Spark procesa una sola vez todos los datos disponibles en el momento y luego detiene automáticamente la ejecución del stream. (Es un stream "por lotes único".)

// Ejercicio 4: Utiliza como origen del Stream, el fichero /home/bigdata/microcuento.txt
// cp /home/bigdata/microcuento.txt /home/bigdata/Plantillas/

// Solución:
val lines = spark.readStream.format("text").load("/home/bigdata/Plantillas/")
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()
wordCounts.writeStream.outputMode("complete").format("console").option("truncate", false).start()

//INPUT: -File-, Kafka, -Socket-, Rate, RatePerMicro-Batch
//MODE: -Append-, -Complete-, -Update-
//SINKS: File, Kafka, -Foreach-, -ForeachBatch-, -Console-, -Memory-
//TRIGGERS: -ProcessingTime-, -Once-, -AvailableNow-

//confluent local services kafka start

// Ejercicio 5: Inicia la Spark-shell incluyendo el artefacto de spark-sql-kafka
// y repite el ejercicio 4 con un topic de Kafka como Sink
// Se debe mostrar el resultado con un consumidor de Kafka que muestre las claves de cada mensaje
// bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3
import org.apache.spark.sql.types._
val lines = spark.readStream.format("text").load("/home/bigdata/Plantillas/")
val words = lines.as[String].flatMap(_.split(" "))
// Solución:
val wordCounts = words.groupBy("value").count().select(col("value").as("key"), col("count").cast(StringType).as("value"))
wordCounts.writeStream.outputMode("complete").format("kafka").option("checkpointLocation", "/tmp/checkpoint/").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "topic1").start()
