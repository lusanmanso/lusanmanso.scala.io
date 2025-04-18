#import "@preview/ilm:1.4.0": *

#set text(lang: "es",size: 12pt)
#set par(justify: true, leading: 1.5em)

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

=== Postgres
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
    -_ Cores in use:_ Total de cores asignados a executors de aplicaciones en ejecución / Total de cores disponibles en todos los workers activos.
    - _Memory in use:_ Total de memoria asignada a executors / Total de memoria disponible en todos los workers activos.
    - _Applications:_ Número de aplicaciones en ejecución / Número de aplicaciones completadas.
    - _Driver Programs:_ Número de drivers en ejecución / Número de drivers completados (relevante en modo cluster).
    - _Status:_ Estado del Master #highlight[(debería ser ALIVE)].

    - _*Workers:*_ Una tabla detallada de cada worker conectado.
      - _Worker Id:_ Identificador único del worker.
      - _Address:_ Dirección IP/hostname y puerto del worker.
      - _State:_ Estado actual.
      - _Cores:_ Cores utilizados / Cores totales de ese worker.
      - _Memory:_ Memoria utilizada / Memoria total de ese worker.

    - _*Running Applications:*_ Tabla de las aplicaciones Spark que están actualmente conectadas al Master y ejecutando (o esperando) tareas.
      - _Application ID:_ Identificador único de la aplicación.
      - _Name:_ Nombre de la aplicación
      - _Cores:_ Cores actualmente otorgados a la aplicación.
      - _Memory per Executor:_ Memoria asignada a cada executor de esa aplicación.
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
    --executor-memory 1g \
    --driver-memory 1g \
    --jars /home/bigdata/postgresql-42.7.5.jar \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4

    # Los datos coinciden con los workers levantados anteriormente
    # .jar descargado previamente
    # Versión más reciente hasta la fecha de Mongo Spark Connector
  ```

  _Nota: Para saber la URL del Master bastaría con ver los logs: _
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
Carga, Organización y Validación de Datos


= Analisis de Datos en el Data Lake
Exploración, Limpieza y Preparación de Datos para Análisis
