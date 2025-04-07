bigdata> cd /home/bigdata/spark-3.3.3-bin-hadoop3/
bigdata> ls sbin/
bigdata> sbin/start-master.sh --help
bigdata> cat conf/spark-defaults.conf.template
bigdata> tail logs/spark-bigdata-org.apache.spark.deploy.master.Master-1-Big-Data.out
bigdata> tail logs/spark-bigdata-org.apache.spark.deploy.master.Master-1-Big-Data.out
firefox> http://10.0.2.15:8080
bigdata> cat conf/workers.template
bigdata> sbin/start-slave.sh spark://Big-Data:7077
bigdata> sbin/stop-slave.sh
bigdata> sbin/start-slave.sh spark://Big-Data:7077 --cores 2 --memory 1g
bigdata> cat conf/spark-env.sh.template
firefox> https://spark.apache.org/docs/3.3.4/spark-standalone.html#cluster-launch-scripts
bigdata> vi conf/spark-env.sh
			SPARK_WORKER_CORES=2
			SPARK_WORKER_INSTANCES=2
			SPARK_WORKER_MEMORY=1g
bigdata> sbin/start-slaves.sh spark://Big-Data:7077
bigdata> jps

// Conectar Spark Driver de la Shell al Gestor de recursos Standalone
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 1G --total-executor-cores 2 --executor-cores 2
spark> sc.isLocal
spark> sc.parallelize(Range(0, 100)).filter(_%2==0).collect()
spark> :q

// EJERCICIO 1: Levanta una aplicación shell que utilice un core de cada worker
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 1G --total-executor-cores 2 --executor-cores 1
spark> sc.parallelize(Range(0, 100)).filter(_%2==0).collect()
spark> :q

// EJERCICIO 2: Lanza los siguientes comandos y explica lo que ocurre
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 16G --total-executor-cores 8 --executor-cores 2
spark> sc.parallelize(Range(0, 100)).filter(_%2==0).collect()
spark> :q
// El Gestor de recursos no puede darle a la aplicación todos los recursos que pide. Por tanto, se levanta el Spark Driver pero el SparkContext no puede ejecutar nada porque está a la espera de que el Gestor de recursos levante los Executors requeridos

// EJERCICIO 3: Lanza los siguientes comandos y explica lo que ocurre
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 1G --total-executor-cores 8 --executor-cores 2
spark> sc.parallelize(Range(0, 100)).filter(_%2==0).collect()
// El gestor de recursos le oferta un executor de 2 cores y 1G y el Spark Driver lo acepta

// EJERCICIO 4:
// Lee https://spark.apache.org/docs/3.3.4/configuration.html#scheduling (Parámetros spark.scheduler.minRegisteredResourcesRatio y spark.scheduler.maxRegisteredResourcesWaitingTime)
// ¿Qué ocurre cuando lanzo una shell con --conf spark.scheduler.minRegisteredResourcesRatio=1.0?
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 16G --total-executor-cores 8 --executor-cores 2 --conf spark.scheduler.minRegisteredResourcesRatio=1.0
// El Gestor de recursos no puede levantar ningún Executor de 16GB

// EJERCICIO 5: ¿Qué pasará si lanzo una shell con --executor-memory 1G y --total-executor-cores 6?
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 1G --total-executor-cores 6 --executor-cores 2 --conf spark.scheduler.minRegisteredResourcesRatio=1.0
// El Spark Driver espera 30s a ver si el Gestor de recursos le da el 100% de los recursos que ha pedido

// EJERCICIO 6: ¿Qué pasará si lanzo una shell con --conf spark.scheduler.minRegisteredResourcesRatio=0.5?
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 1G --total-executor-cores 6 --executor-cores 2 --conf spark.scheduler.minRegisteredResourcesRatio=0.5
// El Gestor de recursos le ofrece al Spark Driver un clúster que cumple la mitad de los recursos pedidos antes de los 30 segundos


// Dynamic Allocation
// EJERCICIO 7: Lanza el siguiente escenario y explica qué ocurre y por qué
bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 1G --total-executor-cores 4 --executor-cores 2 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.shuffleTracking.enabled=true --conf spark.dynamicAllocation.executorIdleTimeout=60s --conf spark.dynamicAllocation.minExecutors=0 --conf spark.dynamicAllocation.initialExecutors=0
spark> sc.parallelize(Range(0, 100000000), 64).filter(_%2==0).collect()
// Una vez que termina el Job de Spark y transcurren 60 segundos, se paran todos los executors por inactividad de tareas.

// Scheduler pools
// EJERCICIO 8: Lanza los siguientes escenarios y explica qué diferencias observas y por qué se producen
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 1G --total-executor-cores 4 --executor-cores 2
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
sc.setLocalProperty("spark.scheduler.pool", null)
Future{
	sc.setLocalProperty("spark.scheduler.pool", null)
	sc.parallelize(Range(0, 100), 40).mapPartitions{x => Thread.sleep(1000); x}.filter(_%2==0).map(_*2).distinct().mapPartitions{x => Thread.sleep(1000); x}.collect()
}.onSuccess{case array => println(array.mkString(",")); println("Job 1 Finished")}
Thread.sleep(100)
Future{
	sc.setLocalProperty("spark.scheduler.pool", null)
	sc.parallelize(Range(0, 100), 40).mapPartitions{x => Thread.sleep(1000); x}.filter(_%2==1).map(_*2).distinct().collect()
}.onSuccess{case array => println(array.mkString(",")); println("Job 2 Finished")}


bigdata> bin/spark-shell --master spark://Big-Data:7077 --deploy-mode client --driver-memory 1G --executor-memory 1G --total-executor-cores 4 --executor-cores 2 --conf spark.scheduler.mode=FAIR
spark> import scala.concurrent._
spark> import scala.concurrent.ExecutionContext.Implicits.global
spark> Future{
	sc.setLocalProperty("spark.scheduler.pool", "production")
	sc.parallelize(Range(0, 100), 40).mapPartitions{x => Thread.sleep(1000); x}.filter(_%2==0).map(_*2).distinct().mapPartitions{x => Thread.sleep(1000); x}.collect()
}.onSuccess{case array => println(array.mkString(",")); println("Job 1 Finished")}
spark> Thread.sleep(100)
spark> Future{
	sc.setLocalProperty("spark.scheduler.pool", "test")
	sc.parallelize(Range(0, 100), 40).mapPartitions{x => Thread.sleep(1000); x}.filter(_%2==1).map(_*2).distinct().collect()
}.onSuccess{case array => println(array.mkString(",")); println("Job 2 Finished")}

// En el primer caso, se lanzan 2 Jobs en paralelo con el Scheduler FIFO (por defecto). Por tanto, el segundo Job no empieza a ejecutarse hasta que el primero Job libera algunos cores porque no los necesita para seguir ejecutándose.
// En el segundo caso, se lanzan 2 Jobs en paralelo con el Scheduler FAIR. Por tanto, el segundo Job empieza a ejecutarse después de unos instantes ya que el Scheduler FAIR toma algunos cores del primer Job a pesar de que este los fuera a utilizar para paralelizar más sus tareas.

// History server (https://spark.apache.org/docs/3.3.4/monitoring.html#viewing-after-the-fact)

// EJERCICIO 9: Arranca HDFS y genera la ruta /shared/spark-logs con una única instrucción
bigdata> cd /home/bigdata/hadoop-3.3.6/
bigdata> sbin/start-dfs.sh
bigdata> bin/hdfs dfs -mkdir -p /shared/spark-logs


bigdata> /home/bigdata/spark-3.3.3-bin-hadoop3
bigdata> cp conf/spark-defaults.conf.template conf/spark-defaults.conf
bigdata> vi conf/spark-defaults.conf
			spark.history.fs.logDirectory   hdfs://localhost:9000/shared/spark-logs
bigdata> sbin/start-history-server.sh
firefox> http://10.0.2.15:18080

// EJERCICIO 10: ¿Cómo arrancarías una shell de Spark para que utilice el history server que se ha lanzado?
bigdata> bin/spark-shell --master spark://Big-Data:7077 --driver-memory 1G --executor-memory 1G --total-executor-cores 4 --executor-cores 2 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://localhost:9000/shared/spark-logs


// Parar procesos de Spark
bigdata> sbin/stop-history-server.sh
bigdata> sbin/stop-all.sh
