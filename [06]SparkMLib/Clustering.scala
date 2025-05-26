// Sepal Length (cm): Length of the sepal in centimeters.
// Sepal Width (cm): Width of the sepal in centimeters.
// Petal Length (cm): Length of the petal in centimeters.
// Petal Width (cm): Width of the petal in centimeters.
// Species: The species of the flower.

// wget -P /home/bigdata/Descargas/ https://gist.githubusercontent.com/mafernandez-stratio/786e1899cfc6b434c12a36cc2ee03c48/raw/507bd4bab77d299e86607822d80c041065ca46d0/iris_dataset.csv

import org.apache.spark.sql._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.evaluation.ClusteringEvaluator


import spark.implicits._

val df = spark...

// ¿Cuántos registros y cuántas columnas?
(df..., df...)
// ¿Nombres de columnas?
df...
// ¿Esquema?
df...
df.orderBy(rand()).show(10, false)
// ¿Cuántas especies distintas hay?
df...
// Ordena de forma descente las especies en función del número de registros
df...

// Realiza una clusterización para obtener el siguiente resultado

clusteredData.orderBy(rand()).show(5, false)

/*
+------------+-----------+------------+-----------+----------+-----------------+----------+
|sepal_length|sepal_width|petal_length|petal_width|species   |features         |prediction|
+------------+-----------+------------+-----------+----------+-----------------+----------+
|6.6         |2.9        |4.6         |1.3        |versicolor|[6.6,2.9,4.6,1.3]|0         |
|5.6         |2.5        |3.9         |1.1        |versicolor|[5.6,2.5,3.9,1.1]|0         |
|7.6         |3.0        |6.6         |2.1        |virginica |[7.6,3.0,6.6,2.1]|2         |
|5.0         |3.5        |1.3         |0.3        |setosa    |[5.0,3.5,1.3,0.3]|1         |
|6.0         |2.2        |4.0         |1.0        |versicolor|[6.0,2.2,4.0,1.0]|0         |
+------------+-----------+------------+-----------+----------+-----------------+----------+
*/

// ¿Cuántos registros hay por cada predicción?
clusteredData...
// Calcula el número de registros de cada predicción por especie
clusteredData...

// Utiliza ClusteringEvaluator para determinar que número de clústeres es más acertado
for (k <- 2 to 9) {
  val kmeans = ...
  val model = ...
  val predictions = ...
  val evaluator = ...
  val silhouette = ...
  println(s"Silhouette with squared euclidean distance(k=$k) = $silhouette")
}
