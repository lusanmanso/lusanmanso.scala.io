#import "@preview/ilm:1.4.0": *

#set text(lang: "es",size: 12pt)
#set par(justify: true, leading: 1.5em)
#show link: underline

#show: ilm.with(
  title: [Análisis de Tráfico Aéreo],
  author: "Lucía Sánchez Manso",
  abstract: [Procesamiento de Datos: Práctica Obligatoria | MAIS],

  date: datetime(year: 2025, month: 5, day: 5),
  figure-index: (enabled: true),
  table-index: (enabled: true),
  listing-index: (enabled: true),
)

= PoC (Proof of Concept)
Comprobar la viabilidad del proyecto y realizar gran parte del desarrollo funcional.

Enlace al #link("https://github.com/lusanmanso/lusanmanso.scala.io/tree/main/PracticaObligatoria")[repositorio de GitHub].

Enlace al #link("https://typst.app/project/runDlZXdiQHL9jcUlEcJsA")[archivo de Typst].

= Preparación del Entorno de Trabajo
Configuración de la Máquina Virtual y Entorno de Ejecución.

== MongoDB
El objetivo es instalar MongoDB 8.0 Community Edition LTS en la máquina virtual, arrancarlo y utilizar su shell.

*Prerequisitos:*
- Ubuntu 22.04 (Jammy)
- Disponer de `gnupg` y `curl` (`sudo apt-get install gnupg curl`)

  #underline[Paso 1: Importar Key Pública]
  ```bash
  curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor
  ```
  #image("api_key.png")

  #underline[Paso 2: Crear el list file]
  ```bash
  echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
  ```
  `Output:`
  ```sh
  deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse

  ```

  #underline[Paso 3: Recargar la lista de paquetes]
  ```bash
  sudo apt-get update
  ```

  #underline[Paso 4: Instalar MongoDB Community Server]
  ```bash
  sudo apt-get install -y mongodb-org
  ```

- Con MongoDB ya instalado, se despliega y se prueba con `mongosh`

  #underline[Paso 1: Arrancar MongoDB Service y comprobar su estado]
  ```bash
  sudo systemctl start mongod
  sudo systemctl status mongod
  ```
  #image("mongod.png")

  #underline[Paso 2: Iniciar Mongo Shell]
  ```bash
  mongosh
  ```
  #image("mongosh.png")

  #underline[Paso 3: Create database `practica` y comprobar]
  ```bash
  use practica
  db.dummy.insertOne({ temp:true })
  db.dummy.deleteMany({})
  show dbs
  ```

  #image("db_dummy.png")

  #underline[Paso 4: Salir de la Mongo Shell]
  ```as
  exit
  ```

  Parar MongoDB Service _(A nivel informativo para parar el servicio de forma ordenada al terminal la práctica)._
  ```bash
  sudo service mongod stop
  ```

== HDFS (Hadoop Distributed File Sytem)
El objetivo es desplegar el sistema de archivos HDFS en la máquina virtual. _Se utilizará el ya instalado en la VM usada en clase._

*Prerequisitos:*
- Hadoop 3.3.6

  #underline[Paso 1: Arrancar]
  ```bash
  cd hadoop-3.3.6/
  sbin/start-dfs.sh
  ```

  #underline[Paso 2: Comprobar levantamiento del servicio]

  Al usar `jps` en la terminal de Ubuntu. _Java Virutal Machine Process Status Tool_ es una herramienta incluida en el JDK de Java que muestra una lista de procesos Java que se están ejecutando en la máquina.

  ```as
  3090 NameNode
  3494 SecondaryNameNode
  3323 DataNode
  ```

  Para cada proceso Java en ejecución, `jps` muestra:
  - El ID del proceso (PID)
  - El nombre de la clase principal o servicio

  En este caso el *NameNode* (NN), *SecondaryNode* (SN) y el *DataNode* (DN) (los servicios HDFS), que demuestra que el servicio está levantado correctamente.

== Postgres
El objetivo utilizar Postgres en la máquina virtual.

*Prerequisitos:*
- Postgres 17.4

  #underline[Paso 1: Comprobar que está arrancado]
  ```bash
  systemctl status postgresql
  ```
  #image("postgres.png")

  PostgreSQL se arranca automáticamente porque su servicio `systemd` se habilita durante la instalación, asegurando que la base de datos esté disponible desde el inicio del sistema, no sólo tras iniciar sesión.

  #underline[Paso 2: Arrancar la Postgres Shell]
  ```sh
  sudo -i -u postgres
  psql
  ```

== Spark Standalone
El objetivo es configurar y realizar el despliegue de un gestor de recursos para arrancar un clúster de Spark utilizando los recursos de este gestor de recursos.

*Prerequisitos:*
- Spark 3.3.3

  #underline[Paso 1: Arrancar y desplegar 1 Master]

  ```sh
  cd spark-3.3.3-bin-hadoop3/ # Navegar al directorio en el que se encuentra Spark
  sbin/start-master.sh
  ```

  #image("start-master.png")

  _Nota: Por defecto, el puerto es el 7077._

  #underline[Paso 2: Arrancar y desplegar 2 Workers]

  El arranque será estandarizado con scripts. En el `conf/spark-env.sh` configura los componentes de cada worker por defecto.

  ```sh
  export SPARK_WORKER_CORES=2 # Cores por cada instancia de worker
  export SPARK_WORKER_MEMORY=1g # Memoria por cada instancia de worker
  export SPARK_WORKER_INSTANCES=2 # Esto lanza 2 workers EN CADA MÁQUINA listada en conf/workers.
  ```
  _Nota: Los recursos (cores y memoria) dependerán de los recursos que tenga la máquina virtual._

  El archivo `conf/workers` especifica donde se quiere que se ejecuten los workers.
  ```sh
  localhost # En este caso queremos a ambos en localhost
  ```

  Al ejecutar `sbin/start-workers.sh` este script leerá `conf/workers` y `conf/spark-env.sh` para iniciar los workers configurados.

  #image("start-workers.png")

  Se puede comprobar ejecutando nuevamente `jps` para ver los procesos.
  ```sh
  8387 Jps
  7621 Master # El proceso Master
  8166 Worker # Los procesos Trabajadores (o Esclavos)
  8215 Worker
  ```

  #underline[Paso 2: Acceder a la WebUI de Spark Standalone]

  Comprobar que el clúster se ha desplegado correctamente es tan fácil como acceder a `http://localhost:8080/` _(desde la VM donde lo se especificó `localhost`)._

  #image("webUi_spark.png")

  La WebUI del Master Standalone te proporciona información clave sobre el clúster:

  - *_Cluster Summary:_*
    - _Worker Url:_ La URL del Master a la que se conectan los workers y las aplicaciones.
    - _Alive Workers:_ Número de workers actualmente conectados y activos.
    - _Cores in use:_ Total de cores asignados a executors de aplicaciones en ejecución / Total de cores disponibles en todos los workers activos.
    - _Memory in use:_ Total de memoria asignada a executors / Total de memoria disponible en todos los workers activos.
    - _Applications:_ Número de aplicaciones en ejecución / Número de aplicaciones completadas.
    - _Driver Programs:_ Número de drivers en ejecución / Número de drivers completados (relevante en modo cluster).
    - _Status:_ Estado del Master

  - _*Workers:*_ Una tabla detallada de cada worker conectado.

  - _*Running Applications:*_ Tabla de las aplicaciones Spark que están actualmente conectadas al Master y ejecutando (o esperando) tareas.

_Nota: Al hacer clic en el nombre de una aplicación, lleva a la WebUI propia de la aplicación (que se ejecuta en el Driver), donde se puede ver todo con más detalle._

  - _*Completed Applications:*_ Similar a "Running Applications", pero lista las aplicaciones que ya han finalizado (correctamente o con error).

  - _*Running Drivers / Completed Drivers:*_ Secciones específicas para los procesos Driver cuando se lanzan en modo cluster directamente gestionados por el Master Standalone.

== Spark Shell
El objetivo es arrancar una shell de Spark formando un clúster de 2 executors a través del uso del gestor de recursos desplegado en el paso anterior.

*Prerequisitos:*
- Conector JDBC de Postgres _(se especificará posteriormente en `--jar`)._
  ```sh
  wget https://jdbc.postgresql.org/download/postgresql-42.7.5.jar # La versión más reciente a esta fecha
  ```
- Conector Spark de MongoDB _(se especificará posteriormente en `--packages`)._

  #underline[Paso 1: Construir comando `spark-shell`]
  ```sh
  bin/spark-shell \
    --master spark://bigdata:7077 # URL del Master
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 2g \
    --driver-memory 1g \
    # --jar /home/bigdata/postgresql-42.7.5.jar \
    # --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

    # Los datos coinciden con los workers levantados anteriormente
    # .jar descargado previamente
    # Versión más reciente hasta la fecha de Mongo Spark Connector no funciona, downgradear.
  ```
  _Nota 1: `.jar` y `mongo-spark-connector` fueron copiados manualmente posteriormente debido a un error contínuo._

  _Nota 2: Para saber la URL del Master bastaría con ver los logs: _
  ```sh
  bigdata@bigdata:~/spark-3.3.3-bin-hadoop3$ tail logs/spark-bigdata-org.apache.spark.deploy.master.Master-*.out # Ejecutando esto salen los logs
  25/04/18 19:06:11 INFO SecurityManager: Changing modify acls groups to:
  25/04/18 19:06:11 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(bigdata); groups with view permissions: Set(); users  with modify permissions: Set(bigdata); groups with modify permissions: Set()
  25/04/18 19:06:11 INFO Utils: Successfully started service 'sparkMaster' on port 7077.
  # Esta es la línea donde se aprecia el URL
  25/04/18 19:06:11 INFO Master: Starting Spark master at spark://bigdata:7077
  25/04/18 19:06:11 INFO Master: Running Spark version 3.3.3
  25/04/18 19:06:11 INFO Utils: Successfully started service 'MasterUI' on port 8080.
  # ...
  ```

  #image("spark_shell.png")

  #underline[Paso 2: Acceder  a la WebUI de Spark Application]

  Acceder a la interfaz web de al aplicación es tan sencillo como acceder a `http://localhost:4040`.

  #image("spark_application.png")

  La WebUI de la Spark Application te proporciona información detallada sobre la sesión de `spark-shell`.

  En este caso, el usuario responsable (_*User*_), el tiempo que ha estado levantada (_*Total Uptime*_), y el modo de programación (*_Scheduling Mode_*), en este caso está el de por defecto FIFO (_First In First Out_).

  #image("executors_added.png")

  Aquí se puede apreciar el momento en el que los dos executors fueron añadidos al levantar la sesión.


= Ingesta de Datos en el Data Lake
Carga, Organización y Validación de Datos | _Los datos de este proyecto están basados en #link("https://openflights.org/data.php")[Openflights]_

== Countries
Los datos sobre países *(`countries.txt`)* están en un formato no estándar `(name::<name> ## iso_code::<iso_code> ## dafif_code::<dafif_code>)`. Se trataron como desestructurados (RDDs) y se realizaron una serie de transformaciones con el objetivo de limpiar los datos y darles una estructura `(name: string, iso_code: string, dafif_code: string)` para obtener un Dataframe.

#underline[Paso 1: Cargar *countries.txt* en el RDD de Spark]

```scala
val rdd_inicial = sc.textFile("/home/bigdata/Descargas/practica/countries.txt")
```
#image("cargar_countries.png")

#underline[Paso 2: Transformaciones para compatibilizar estructura con Dataframe]

```scala
val rdd_transformado = rdd_inicial.map { linea =>
  val partes = linea.split(" ## ") // Divide la línea por " ## "
  // Extrae cada valor dividiendo por "::" y tomando el segundo elemento
  val nombre = partes(0).split("::")(1)
  val iso = partes(1).split("::")(1)
  val dafif = partes(2).split("::")(1)
  (nombre, iso, dafif) // Devuelve tupla con los valores
}
```

#image("transformar_countries.png")

```scala
import spark.implicits._ // Conversiones implícitas -> usar .toDF()
// Define los nombres de las columnas
val columnas = Seq("name", "iso_code", "dafif_code")
// Convierte el RDD de tuplas a un DataFrame especificando los nombres de columna
val df_countries = rdd_transformado.toDF(columnas: _*)
df_countries.printSchema()
df_countries.show(5, false) // false para no truncar columnas largas
```

#image("dataframe_countries.png")

#underline[Paso 3: Escribir los datos `.csv` en `/practica/countries` de HDFS]

```scala
val ruta_destino_hdfs = "hdfs://localhost:9000/practica/countries/"

df_countries.write.option("header", "true").mode("overwrite").csv(ruta_destino_hdfs)
```

#image("countries_csv.png")

_Nota: Para encontrar el puerto donde está lanzado el HDFS basta con acceder a `core_site.xml` y buscar el puerto enlazado en la pantalla._

#underline[Paso 4: Ejemplo de datos almacenados]

```scala
val df_leido_hdfs = spark.read.option("header", "true").option("inferSchema", "true").csv(ruta_destino_hdfs)

df_leido_hdfs.show(5)
```

_Nota: La variable de ruta en las capturas es `ruta_destino_hdfs_correcta` porque me equivoqué la primera vez._

#image("countries_comprobacion.png")

== Airlines

Los datos sobre aerolíneas *(`airlines.dat`)* tienen un columna (`active`) que requiere una transformación: se deberá leer inicialmente como un String, pero a la cual se le deberá aplicar una serie de transformaciones para cambiar sus valores (n, Y, N) a un valor booleano (true o false) y así actualizar el esquema para que la columna active sea de tipo Boolean.

#underline[Paso 1: Cargar *airlines.dat* como DataFrame]
```scala
// Imports necesarios para las operaciones posteriores
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType

val ruta_airlines_dat = "/home/bigdata/Descargas/practica/airlines.dat"

val df_airlines_check = spark.read.csv(ruta_airlines_dat)
```

#image("airlines_load.png")

#underline[Paso 2: Transformar la columna 'active' a Boolean]

```scala
val df_airlines_preparado = df_airlines_check.select(
    col("_c0").alias("airline_id"),
    col("_c1").alias("name"),
    col("_c2").alias("alias"),
    col("_c3").alias("iata"),
    col("_c4").alias("icao"),
    col("_c5").alias("callsign"),
    col("_c6").alias("country"),
    // Transformamos _c7 directamente a la columna booleana 'active'
    when(upper(col("_c7")) === "Y", true).otherwise(false).cast(BooleanType).alias("active")
  )
```

#image("airlines_transformado.png")

#underline[Paso 3: Cargar datos de Countries desde HDFS para el lookup]

```scala
val ruta_countries_hdfs = "hdfs://localhost:9000/practica/countries/"

val df_countries_lookup = spark.read.option("header", "true").option("inferSchema", "true").csv(ruta_countries_hdfs).select(col("name").alias("country_name"),col("iso_code"))

df_countries_lookup.show(3)
```

#image("airlines_loaded.png")

Además, se requiere enriquecer los datos de este Dataframe, añadiendo una nueva columa `country_iso` que corresponde al iso_code correspondiente al país indicado en la columna country.

#underline[Paso 4: Añadir columna `country_iso` mediante JOIN]

```scala
val df_airlines_final = df_airlines_preparado.join(df_countries_lookup, df_airlines_preparado("country") === df_countries_lookup("country_name"), "left_outer").withColumnRenamed("iso_code", "country_iso").drop("country_name").select("airline_id", "name", "alias", "iata", "icao", "callsign", "country", "active", "country_iso")

df_airlines_final.printSchema() // Esquema
df_airlines_final.show(5, false) // Lineas del df
```

#image("airlines_join.png")

Una vez obtenido este Dataframe, se debe utilizar el conector parquet para guardar los datos particionándolos por la columna country en el `path /practica/airlines/` de HDFS.

#underline[Paso 5: Escribir DataFrame final en HDFS (Parquet, particionado)]

```scala
val ruta_salida_airlines = "hdfs://localhost:9000/practica/airlines/"

df_airlines_final.write.partitionBy("country").mode("overwrite").parquet(ruta_salida_airlines)
```

#underline[Paso 6: Verificar los datos leídos desde HDFS]

```scala
val df_airlines_leido_hdfs = spark.read.parquet(ruta_salida_airlines)

df_airlines_leido_hdfs.show(5, false)
```

#image("airlines_hdfs.png")

== Airports

Los datos sobre aeropuertos *(`airports.dat`)* se pueden cargar en un Dataframe con el conector correspondiente y las opciones que se
requieran.

Se debe de realizar la transformación a metros de la columna altitude de pies a metros porque #highlight[*nadie en su sano juicio usa el sistema imperial*] y con una UDF aplicarla a dicha columna.

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties // Para las propiedades JDBC
```

#underline[Paso 1: Desarrollo y registro de UDF (Pies a Metros)]

```scala
val feetToMeters = (feet: Double) => { feet * 0.3048 }

spark.udf.register("feetToMetersUDF", feetToMeters)
```

#image("airport_udf.png")

#underline[Paso 2: Cargar *airport.dat* como Dataframe]

```scala
val ruta_airports_dat = "/home/bigdata/Descargas/practica/airports.dat"

// Nota: Se lee altitud (_c8), latitud (_c6), longitud (_c7) como String inicialmente para manejar posibles valores no numéricos antes de la conversión.
val airportsSchema = StructType(Array(
  StructField("airport_id", IntegerType, true),
  StructField("name", StringType, true),
  StructField("city", StringType, true),
  StructField("country", StringType, true),
  StructField("iata", StringType, true),
  StructField("icao", StringType, true),
  StructField("latitude_str", StringType, true),
  StructField("longitude_str", StringType, true),
  StructField("altitude_str", StringType, true),
  StructField("timezone", StringType, true),
  StructField("dst", StringType, true),
  StructField("tz_database_time_zone", StringType, true),
  StructField("type", StringType, true),
  StructField("source", StringType, true)
))
```

#image("airport_load.png")

```scala
// Carga de CSV sin cabecera y aplicando el esquema
val df_airports_raw = spark.read.schema(airportsSchema).csv(ruta_airports_dat)

df_airports_raw.printSchema()
```

#image("airport_schema.png")

#underline[Paso 3: Utilización de la UDF `feetToMeter` para convertir `altitude`]

_Nota: Descartamos altitude_str, latitude_str, longitude_str y altitude_double._

```scala
val df_airports_transformed = df_airports_raw
  // 1. Convertir altitude_str a Double (temporalmente)
  .withColumn("altitude_double", col("altitude_str").cast(DoubleType))
  // 2. Aplicar UDF para convertir pies a metros
  .withColumn("altitude_meters", callUDF("feetToMetersUDF", col("altitude_double")))
  // 3. Convertir latitude_str y longitude_str a Double
  .withColumn("latitude", col("latitude_str").cast(DoubleType))
  .withColumn("longitude", col("longitude_str").cast(DoubleType))
  // 4. Seleccionar las columnas deseadas para el DataFrame final
  .select(col("airport_id"), col("name"), col("city"), col("country"), col("iata"), col("icao"), col("latitude"), col("longitude"), col("altitude_meters"), col("timezone"), col("dst"), col("tz_database_time_zone"), col("type"), col("source"))

// Comprobar el esquema resultante
df_airports_transformed.printSchema()
```
#image("airport_transformed.png")

Una vez obtenido este Dataframe, se debe utilizar el conector JDBC para guardar los datos en la tabla airports de Postgres.

_Nota: Al hacer la lectura de estos datos, se deben añadir las propiedades `partitionColumn`, `lowerBound`, `upperBound` y `numPartitions` propias del conector JDBC._

#underline[Paso 4: Escritura a PostgreSQL usando JDBC]

```sh
# Prerequisito: Cambiar la contraseña de usuario postgres si es necesario
sudo -i -u postgres
[ sudo ] contraseña para bigdata
postgres@bigdata: ~ psql
psql (14.17 (Ubuntu 14.17-0ubuntu0.22.01.1))
Type "help" for help

postgres=# ALTER USER postgres PASSWORD 'pass';
ALTER ROLE
postgres=# \q
postgres@bigdata: ~$ exit
cerrar sesión
```
---
```scala
// -- Definición de variables para el entorno --
val jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
val db_table = "airports" // Nombre de la tabla destino en Postgres
val db_user = "postgres"
val db_password = "pass"

val connectionProperties = new Properties()
connectionProperties.put("user", db_user)
connectionProperties.put("password", db_password)
connectionProperties.put("driver", "org.postgresql.Driver")

// Nota: Se definió tamaño de lote reducido para intentar evitar problemas de memoria
val batch_size = 100
// Escribir el dataframe
df_airports_transformed.write.option("batchsize", batch_size).mode("overwrite").jdbc(jdbc_url, db_table, connectionProperties)
```

#underline[Paso 5: Lectura desde PostgreSQL (con particiones)]

*Justificación de los parámetros de partición para la lectura JDBC:*

- *partitionColumn* ("`airport_id`")*:* Se elige porque es una columna numérica (IntegerType),
  requisito para la partición JDBC en Spark. Se espera que los IDs tengan una distribución
  razonablemente uniforme, permitiendo dividir el trabajo eficientemente.

- *lowerBound (1) / upperBound (15000):* Definen el rango total de `airport_id` que Spark considerará. Idealmente, serían el mínimo y máximo real, pero se usan estimaciones razonables (1 a 15000) para cubrir los datos de OpenFlights. Un rango incorrecto puede causar lectura incompleta o desequilibrio en las particiones.

- *numPartitions (4):* Determina en cuántas tareas paralelas se dividirá la lectura del rango. Cada tarea ejecutará una consulta SQL sobre un sub-rango de `airport_id`. Se eligió 4 para coincidir con los cores totales de los executors (2 executors `*` 2 cores = 4), buscando maximizar el paralelismo inicial.

- *Utilidad:* Estos parámetros permiten a Spark leer datos de la tabla JDBC en paralelo,
  evitando que una sola tarea/conexión se convierta en cuello de botella. Acelera significativamente la carga de tablas grandes distribuyendo el trabajo entre los executors.

```scala
val partition_column = "airport_id"
val lower_bound = 1
val upper_bound = 15000
val num_partitions = 4  // Basado en 2 executors * 2 cores = 4
```

```scala
val df_from_postgres = spark.read.jdbc(jdbc_url, db_table,partition_column, lower_bound, upper_bound, num_partitions, connectionProperties)

df_from_postgres.show(5)
```

#image("airport_postgres.png")

== Routes

Los datos sobre rutas (*`routes.dat`*) tienen que subirse en MongoDB para guardar los datos en la tabla `routes` del database practica.

```sh
# Spark Home es el directorio donde se aloja Spark en la VM
SPARK_HOME=~/spark-3.3.3-bin-hadoop3
```

#image("mongo_deps.png")
_Nota: Copiado todo manualmente porque `--packages` daba error._

#underline[Paso 1: Cargar *routes.dat* como Dataframe]

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val ruta_routes_dat = "/home/bigdata/Descargas/practica/routes.dat"

val routesSchema = StructType(Array(
  StructField("airline", StringType, true), // _c0
  StructField("airline_id", StringType, true), // _c1 (ID OpenFlights)
  StructField("source_airport", StringType, true), // _c2 (IATA/ICAO)
  StructField("source_airport_id", StringType, true), // _c3 (ID OpenFlights)
  StructField("dest_airport", StringType, true), // _c4 (IATA/ICAO)
  StructField("dest_airport_id", StringType, true), // _c5 (ID OpenFlights)
  StructField("codeshare", StringType, true), // _c6 ('Y' o null)
  StructField("stops", IntegerType, true), // _c7 (Número de paradas)
  StructField("equipment", StringType, true) // _c8 (Códigos de avión)
))

// Usamos '\\N' para representar los valores nulos, común en este dataset
val df_routes = spark.read.schema(routesSchema).option("nullValue", "\\N").csv(ruta_routes_dat)
```

#image("routes_load.png")

```scala
df_routes.printSchema()
df_routes.show(5, false)
```

#image("route_load_check.png")

#underline[Paso 2: Particionado del Dataframe para la escritura]

```scala
val num_cores_totales = 4 // 2 executors * 2 cores
val num_particiones = num_cores_totales * 3 // Intento con 12 particiones
val df_routes_repartitioned = df_routes.repartition(num_particiones) // Se reasigna
```

#underline[Paso 3: Escritura en MongoDB]

*Prerequisitos:* Asegurar que Mongo está corriendo en nuestra máquina y admite conexión.

```sh
# Revisar estado
sudo systemctl status mongod
# Revisar configuración de bind | puerto
sudo nano /etc/mongod.conf
```
Tiene que aparecer algo similar a esto:
```yaml
net:
  port: 27017
  bindIp: 0.0.0.0
```
---
```scala
// -- Definición de variables para el entorno --
val mongo_uri = "mongodb://localhost:27017"
val mongo_db = "practica"
val mongo_collection = "routes"

// Escritura como tal (Sobreescribe si ya existe)
df_routes_repartitioned.write.format("mongo").mode("overwrite").option("uri", mongo_uri).option("database", mongo_db).option("collection", mongo_collection).save()
```

#underline[Paso 4: Verificar su lectura desde Mongo]

```scala
val df_from_mongo = spark.read.format("mongo").option("uri", mongo_uri).option("database", mongo_db).option("collection", mongo_collection).load()

df_from_mongo.show(5, false)
```

#image("mongo_routes.png")
