
////////////////////////////////////////////////
// 1. CREATING A DF IN SPARK
////////////////////////////////////////////////

val data = Seq(
	("Juan", "ciencias", 7.2),
	("Maria", "matematicas", 8.7),
	("Pedro", "historia", 3.4),
	("Rosa", "ciencias", 6.8),
	("Julia", "historia", 7.6),
	("Luis", "matematicas", 9.5)
)

// 1.1. Creating a dataframe by using toDF()

import spark.implicits._

val mydf = data.toDF("alumno", "asignatura", "calificacion")

// 1.2. Creating a dataframe by defining its structure and using the method createDataFrame

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType

val schema = List(
  StructField("alumno", StringType, true),
  StructField("asignatura", StringType, true),
  StructField("calificacion", DoubleType, true)
)

// Defining a sequence of Rows; once it is parallelized, a RDD of Rows or RDD[Row] is obtained. This is in essence the structure of a dataframe

import org.apache.spark.sql.Row

val data_rows = data.map(t => Row.fromSeq(Seq(t._1,t._2,t._3)))

spark.createDataFrame(sc.parallelize(data_rows), StructType(schema))

val anotherdf = spark.createDataFrame(spark.sparkContext.parallelize(data),StructType(schema))

// 1.3. Read data from csv file
// wget https://gist.githubusercontent.com/mafernandez-stratio/4d65c9ad88b40ca4cbe9f9b7da67d8f7/raw/aa08fee35b3d687990c8bd80192b200f7b148780/winequality-red-white.txt

spark.read.format("csv").load("/home/bigdata/Descargas/winequality-red-white.txt").show()
spark.read.csv("/home/bigdata/Descargas/winequality-red-white.txt").show()


val tmp1df = spark.read.format("csv").option("header", "true").load("/home/bigdata/Descargas/winequality-red-white.txt")

// Problem? Delimiter is not a comma, but a pipe --> Specify "delimiter" option

val tmp2df = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("/home/bigdata/Descargas/winequality-red-white.txt")

// Problem? All the columns as string, but they represent figures. --> Specify "inferSchema" option

val winesdf = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("/home/bigdata/Descargas/winequality-red-white.txt")

////////////////////////////////////////////////
// 2. DF STRUCTURE (METADATA)
////////////////////////////////////////////////

// 2.1. Método printSchema

winesdf.printSchema

// 2.2. Método dtypes

winesdf.dtypes

// 2.3. Método columns

winesdf.columns

////////////////////////////////////////////////
// 3. FROM DATAFRAME TO RDD
////////////////////////////////////////////////

val myrdd = mydf.rdd.map(r => (r.getAs[String]("alumno"), r.getAs[Double]("calificacion")))

myrdd.first()

////////////////////////////////////////////////
// 4. INITIAL EXPLORATION OF THE DATAFRAME
////////////////////////////////////////////////

// Inspeccionamos su contenido

winesdf.show()

winesdf.show(10)

winesdf.show(10, truncate=false)

// Exploramos con mayor detenimiento algunas variables

winesdf.describe("fixed_acidity", "density").show

// Número de elementos en el dataframe

winesdf.count()

// Número de graduacioned de alcohol distintas que nos encintramos

winesdf.select("alcohol").distinct().count

////////////////////////////////////////////////
// 5. SOME INITIAL QUERIES
////////////////////////////////////////////////

// 5.1. Creamos una vista temporal de la tabla que desaparecerá al cerrar la sesión

winesdf.createGlobalTempView("wines")

// 5.2. Tras registrar la tabla, podemos lanzar queries SQL mediante el método "sql" del objeto SparkSession

spark.sql("select fixed_acidity, density from global_temp.wines").show

// 5.3. Esta misma query podemos llevarla a cabo mediante la API de Spark

winesdf.select("fixed_acidity", "density").show
