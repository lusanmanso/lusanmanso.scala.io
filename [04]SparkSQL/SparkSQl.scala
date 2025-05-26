//Diferencia entre un Dataframe y un Dataset
//Dataframe (Syntax Errors in Compile Time & Analysis Errors in Runtime)
spark.read.json("/home/bigdata/Descargas/fruits.json")
val df = spark.read.json("/home/bigdata/Descargas/fruits.json")
df.filter("rating > 8").show()
df.filter("ratingg > 8") // Analysis error in Runtime
//Dataset (Syntax Errors in Compile Time & Analysis Errors in Compile Time)
case class Fruit(id: Long, name: String, rating: Double)
val dataset = df.as[Fruit]
dataset.columns
dataset.filter(_.rating > 8).show() // Analysis error in Compile Time

// De RDD a Dataframe
val rdd = sc.parallelize(Range(0, 10)).map(x => (x, s"name $x"))
case class KV(key: Int, value: String)
rdd.map{ case(k,v) => KV(k, v) }
val df = rdd.map{ case(k,v) => KV(k, v) }.toDF()

// De Dataframe a tabla SQL
df.createOrReplaceTempView("KeyValue")
spark.sql("SELECT * FROM KeyValue").show()

//Ejercicio: De RDDs a DataFrame
// wget https://gist.githubusercontent.com/mafernandez-stratio/a694fe539e4e1b14d151a1ede0339f1d/raw/2056783dfcc057f43e880faabdef63781bf8b193/access.log
val lines = sc.textFile("/home/bigdata/Descargas/access.log", 10).map(_.replaceAll("\\[","\"").replaceAll("\\]", "\"")).map(line => line.split("\"").map(_.trim).filter(_.nonEmpty)).filter(_.size > 6)
case class LogInfo(ip: String, col2: String, user: String, fecha: String, url: String, status: Int, code: Int, col8: String, browser: String, col10: String)
val logs = lines.map(line => Array(line(0).split(" "), Array(line(1)), Array(line(2)), line(3).split(" "), Array(line(4)), Array(line(5)), Array(line(6)))).map(_.flatten).filter(_.size > 9)
import scala.util._
val rows = logs.map(arr => LogInfo(arr(0),arr(1),arr(2),arr(3),arr(4),Try(arr(5).toInt).getOrElse(0),Try(arr(6).toInt).getOrElse(0),arr(7),arr(8),arr(9)))
val df = spark.createDataFrame(rows)
df.agg(max("status")).show
df.createOrReplaceTempView("test")
spark.sql("SHOW TABLES").show
spark.sql("SELECT * FROM test LIMIT 1").show(50, false)
spark.sql("SELECT status, avg(code) FROM test GROUP BY status").show(50, false)

//Ejercicio: De DataFrame a Dataset
case class LogLine(ip: String, col2: String, user: String, fecha: String, url: String, status: Int, code: Int, col8: String, browser: String, col10: String)
import spark.implicits._
val ds = df.as[LogLine]
ds.count

//Ejercicio: De Dataset a DataFrame
ds.toDF().count

//Ejercicio: De DataFrame a RDD
df.rdd.count

//Ejercicio: De RDD a Dataset
spark.createDataset(rows)

//Ejercicio: De Dataset a RDD
ds.rdd.count

//Ejercicio 1: Convertir de RDD a Tabla SQL
// wget https://gist.githubusercontent.com/mafernandez-stratio/c1e092f9a0aeb073a35200f4555cea1d/raw/effba761c6bb0a5a86f6bb2171fbfbf95e3509de/emails.txt
// Estructura de cada línea:
// Asunto: $ Fecha: $ Desde: $ Para: $ Mensaje: $
// Pista --> Funciones de Scala: substring & indexOf
// Pregunta: ¿Cuántos emails van dirigidos a usuarios con email de yahoo?
val emailsRDD = sc.textFile("/home/bigdata/Descargas/emails.txt")
val emailsTuplas = emailsRDD.map{ line => (
						line.substring(line.indexOf("Asunto: ")+8, line.indexOf("Fecha: ")-1).trim(),
						line.substring(line.indexOf("Fecha: ")+7, line.indexOf("Desde: ")-1).trim(),
						line.substring(line.indexOf("Desde: ")+7, line.indexOf("Para: ")-1).trim(),
						line.substring(line.indexOf("Para: ")+6, line.indexOf("Mensaje: ")-1).trim(),
						line.substring(line.indexOf("Mensaje: ")+9).trim()
					)
			}
case class	Email(asunto: String, fecha: String, desde: String, para: String, mensaje: String)
val emailsDF = emailsTuplas.map(tupla => Email(tupla._1, tupla._2, tupla._3, tupla._4, tupla._5)).toDF()
emailsDF.createOrReplaceTempView("emails")
spark.sql("SHOW TABLES").show
spark.sql("SELECT COUNT(*) FROM emails WHERE contains(para, 'yahoo')").show

//Ejercicio: UDF
spark.udf.register("extractdomain", (email: String) => { email.dropWhile(c => c!='@').stripPrefix("@") })
spark.sql("SELECT extractdomain(para) FROM emails").show

//Ejercicio 2: UDF extractyear para extraer año de la fecha
spark.udf.register("extractyear", (fecha: String) => { fecha.substring(0, fecha.indexOf("-")) })
spark.sql("SELECT extractyear(fecha) FROM emails").show

//Ejercicio: DataFrame con HDFS+parquet particionado
spark.sql("CREATE TABLE partitionedtable(id INT, rating DOUBLE, country STRING) USING parquet OPTIONS(path 'hdfs://localhost:9000/tmp/test') PARTITIONED BY (country)")
spark.sql("INSERT INTO partitionedtable VALUES(1, 9.2, 'Spain')")
spark.sql("INSERT INTO partitionedtable VALUES(2, 8.9, 'United States')")
spark.sql("DESCRIBE EXTENDED partitionedtable").show(50, false)
spark.sql("SELECT * FROM partitionedtable").show(50, false)
spark.sql("EXPLAIN EXTENDED SELECT * FROM partitionedtable WHERE country='Spain'").show(50, false)

//Ejercicio: DataFrame con CSV
//wget https://gist.githubusercontent.com/mafernandez-stratio/6486eacb387350d8e9b2d5890777dd3a/raw/b83478b6a0471caa4c4abee20c08d2401e380779/winemag.csv
//https://spark.apache.org/docs/3.3.4/sql-data-sources-csv.html
spark.sql("CREATE TABLE winemagcsv USING csv OPTIONS (path '/home/bigdata/Descargas/winemag.csv', header 'true', inferSchema 'true')")
spark.sql("DESCRIBE EXTENDED winemagcsv").show(100, false)
spark.sql("SELECT * FROM winemagcsv").show(50, false)

//Ejercicio 3: Media de puntos de vinos cuyo precio no es nulo
//https://spark.apache.org/docs/latest/api/sql/index.html
spark.sql("SELECT avg(points) FROM winemagcsv WHERE isnotnull(price)").show(50, false)

//Ejercicio 4: Crear tabla a partir del siguiente csv
// wget https://gist.githubusercontent.com/mafernandez-stratio/c97926996ca458e66600215bb47f93b6/raw/9f557a7955c130dda82b640ff512d4f5735e763a/winemag_special_format.csv
// Pregunta: ¿Cuál es el precio del vino más caro de cada país?
spark.sql("CREATE TABLE winemag_quotes USING csv OPTIONS (path '/home/bigdata/Descargas/winemag_special_format.csv', header 'true', inferSchema 'true', quote '$', sep '|', comment '-')")
spark.sql("SELECT country, max(price) FROM winemag_quotes GROUP BY country").show

//Ejercicio: Join
spark.sql("SELECT * FROM partitionedtable, winemagcsv WHERE partitionedtable.id=winemagcsv.id").show(50, false)

//Ejercicio: ETL
spark.sql("CREATE TABLE persistedtest USING parquet OPTIONS (path 'hdfs://localhost:9000/tmp/persistedtest') PARTITIONED BY (status) AS SELECT * FROM test WHERE browser LIKE '%Chrome%'")
spark.sql("DESCRIBE EXTENDED persistedtest").show(50, false)
spark.sql("SELECT * FROM persistedtest LIMIT 1").show(50, false)

//Ejercicio: DataFrame con JSON
// https://www.kaggle.com/datasets/robinsonros/latest-earthquake-dataset-up-to-2024
// wget https://gist.githubusercontent.com/mafernandez-stratio/5cba89aeea095f6afaf4f241b89deec3/raw/14e2fa593c13eef6f3453ced67bc40dafa76dcc1/earthquakes_2024.json
spark.sql("CREATE TABLE jsontable USING json OPTIONS (path '/home/bigdata/Descargas/earthquakes_2024.json')")
spark.sql("SELECT * FROM jsontable LIMIT 1").show(50, false)
spark.sql("DESCRIBE EXTENDED jsontable").show(50, false)
spark.sql("SELECT * FROM jsontable LIMIT 10").printSchema
spark.sql("SELECT id FROM jsontable LIMIT 10").show
spark.sql("SELECT properties.place FROM jsontable LIMIT 10").show(500, false)

//Ejercicio 5: Crear una tabla (formato parquet en HDFS) particionada por tipo (type) de seísmo de
// aquellos terremotos de magnitud (mag) mayor a 4 y que fueran detectados por más de 50 estaciones (nst).
// La tabla debe contener las columnas relativas a lugar(place), tipo (type), magnitud (mag) y número de estaciones (nst)
spark.sql("CREATE TABLE terremotos USING parquet OPTIONS (path 'hdfs://localhost:9000/data/terremotos') PARTITIONED BY (type) AS SELECT properties.place AS place, properties.mag AS mag, properties.nst AS nst, properties.type AS type FROM jsontable WHERE properties.mag > 4 AND properties.nst > 50").show(500, false)
spark.sql("SELECT * FROM terremotos").show(500, false)

// Ejercicio: conector JDBC
// sudo apt install postgresql
// sudo -u postgres psql
// postgres=# ALTER USER postgres PASSWORD 'postgres';
// postgres=# CREATE TABLE mytable(id INT, name VARCHAR, rating INT);
// postgres=# INSERT INTO mytable VALUES(1, 'Clase', 9);
// postgres=# SELECT * FROM mytable;
// postgres=# \q
// wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.7/postgresql-42.3.7.jar
// cp /home/bigdata/.m2/repository/org/postgresql/postgresql/42.3.7/postgresql-42.3.7.jar /home/bigdata/Descargas/
// bin/spark-shell --jars /home/bigdata/Descargas/postgresql-42.3.7.jar
import java.util.Properties
val props = new Properties()
props.setProperty("user", "postgres")
props.setProperty("driver", "org.postgresql.Driver")
props.setProperty("password", "postgres")
val jdbcDF = spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres", "mytable", props)
jdbcDF.show()

// Ejercicio 6: Crea una tabla con esta misma tabla de Postgres con una sentencia SQL
spark.sql("CREATE TABLE pgtable USING jdbc OPTIONS (url 'jdbc:postgresql://localhost:5432/postgres', driver 'org.postgresql.Driver', user 'postgres', password 'postgres', dbtable 'mytable')")
spark.sql("SELECT * FROM pgtable").show
