// Levantar Spark Standalone con API Rest activada
bigdata> cd /home/bigdata/spark-3.3.3-bin-hadoop3/
bigdata> cp conf/spark-defaults.conf.template conf/spark-defaults.conf
bigdata> gedit conf/spark-defaults.conf
			spark.master.rest.enabled	true
bigdata> sbin/start-master.sh
firefox> http://localhost:8080/
bigdata> sbin/start-slaves.sh spark://Big-Data.bigdata.virtualbox.org:7077

// Spark-Submit para desplegar aplicación empaquetada en un fichoer JAR
bigdata> sudo apt-get install git
bigdata> sudo apt-get install maven
bigdata> git clone https://github.com/mafernandez-stratio/spark-very-basic-server.git
bigdata> cd spark-very-basic-server/
bigdata> gedit pom.xml
bigdata> gedit src/main/scala/examples/batch/VowelsCounter.scala
bigdata> mvn package
bigdata> mvn clean compile assembly:single
bigdata> cd ../spark-3.3.3-bin-hadoop3/
bigdata> bin/spark-submit --class examples.batch.VowelsCounter --master spark://Big-Data.bigdata.virtualbox.org:7077 --deploy-mode cluster --driver-memory 1G --driver-cores 2 --executor-memory 1G --total-executor-cores 2 --executor-cores 2 /home/bigdata/spark-very-basic-server/target/very-basic-server-0.1-SNAPSHOT-jar-with-dependencies.jar 9070
bigdata> nc localhost 9070

// Parar aplicación utilizando la API Rest
curl -X POST http://Big-Data.bigdata.virtualbox.org:6066/v1/submissions/kill/<Submission ID>

//¿Cómo cancelarías la aplicación VowelsCounter desde la WebUI del Master de Spark Standalone?
//Botón kill al lado del Application ID dentro de la sección Running Applications

//Ejercicio 1: ¿Cómo cancelarías la aplicación VowelsCounter programáticamente a los 30 segundos modificando el código?

import scala.concurrent._
val MyJobGroup = "VowelsCounter"
implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
Future {
  	Thread.sleep(30000)
	sc.cancelJobGroup(MyJobGroup)
}
sc.setJobGroup(MyJobGroup, "App Counting Vowels")
// Añadir Thread.sleep(60000) en un mapPartitions para result
val result = resultRDD.mapPartitions{iter => Thread.sleep(60000); iter}.collect().mkString("; ")

// Ejercicio 2: Observa lo que ocurre con el driver en la UI de Spark Standalone. ¿Por qué se levanta otro driver?
// Modo Cluster: Vuelve a levantar el Driver si se produce una excepción fatal en el Driver

// Ejercicio 3: ¿Cómo lo solucionarías?
// Añadir tratamiento de excepciones para devolver error
import scala.util._
val result = Try(resultRDD.mapPartitions{iter => Thread.sleep(60000); iter}.collect().mkString("; ")) match {
	case Success(value) => value
    case Failure(exception) => exception.getMessage
}

bigdata> sbin/stop-all.sh
