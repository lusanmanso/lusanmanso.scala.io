import org.apache.spark.sql.functions._

val winesdf = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("/home/bigdata/Descargas/winequality-red-white.txt")


// 1. Obtener una muestra de ejemplo del DF por pantalla

// Método "show" de Dataset

winesdf.show

// 2. Visualizar el esquema o estructura del DF

// Método "printSchema" de Dataset

winesdf.printSchema


// 3. Quedarnos sólo con los nombres de la columnas que albergan datos de tipo "double"

// Método "dtypes" de Dataset

val double_cols = winesdf.dtypes.filter{case(c, t) => t.compareTo("DoubleType")==0}.map(_._1)

// 4. Descripción estadística de las variables “fixed_acidity” y "citric_acid" simultáneamente

// Método "describe" de Dataset

winesdf.describe("fixed_acidity", "citric_acid").show

// 5. Número de muestras total en el dataset?

// Método "count" de Dataset

winesdf.count

// 6. Número de filas distintas en el dataset?

// Método "distinct" y "dropDuplicates" de Dataset

winesdf.distinct().count

winesdf.dropDuplicates().count

// 7. Hay vinos iguales (i.e., hay filas que se repiten)? (True/False)

winesdf.count > winesdf.distinct().count

// 8. Número de nulos en cada columna

// Método "isNull" de Column
// Método "sum" de functions

val all_cols = winesdf.columns

val null_counts = all_cols.map(c => sum(col(c).isNull.cast("int")).alias(c + "_nulls"))

winesdf.select(null_counts:_*).show

winesdf.agg(null_counts.head, null_counts.tail:_*).show

// 9. Cuántos vinos hay de cada tipo: blanco, tinto?

// Método "groupBy" y "agg" de Dataset

winesdf.groupBy("style").agg(count("*")).show

// 10. Comparar tintos vs blancos en términos de acidez (fixed_acidity) y calidad (quality): mean +- std
//    - 10.1. Qué categoría tiende a presentar mayor acidez?
//    - 10.2. En qué categoría los vinos tienden a tener mayor calidad?

// Método "groupBy" y "agg" de Dataset
// Métodos "mean" y "stddev" de functions

winesdf.groupBy("style").agg(count("*"), mean("fixed_acidity"), stddev("fixed_acidity"), mean("quality"), stddev("quality")).show

// 11. Nueva columna "pcg_free_sulfur_dioxide" con el porcentaje de SO2 de tipo libre: col("free_sulfur_dioxide")/col("total_sulfur_dioxide")

// Método "withColumn" de Dataset
// Método col de functions

winesdf.withColumn("pcg_free_sulfur_dioxide", col("free_sulfur_dioxide")/col("total_sulfur_dioxide")).show

// 12. Crear una columna "chloride_per_citric_acid" que represente el nivel de “chlorides” por cada unidad de “citric_acid”. Si el valor de
// esta última es 0, la columna resultante debe ser -1.0

// Método "withColumn" de Dataset
// Método when y col de functions

winesdf.withColumn("chloride_per_citric_acid", when(col("citric_acid") > 0.0, col("chlorides")/col("citric_acid")).otherwise(-1.0)).show

// 13. Nueva columna con "id" del vino

// Método "withColumn" de Dataset
// Método "monotonically_increasing_id" de functions

val winesiddf = winesdf.withColumn("id", monotonically_increasing_id())

winesiddf.show

// 14. Valor medio de “fixed_acidity”?

// Método "select" de Dataset
// Método "mean" de functions

winesdf.select(mean("fixed_acidity")).show

// 15. Recoger el valor medio de “fixed_acidity” en una variable

// Método "first" de Dataset
// Método "getAs" de Row

val mean_fixed_acidity = winesdf.select(mean("fixed_acidity").alias("mean_fixed_acidity")).first.getAs[Double]("mean_fixed_acidity")

// 16. Cuántos vinos hay con un “fixed_acidity” superior a la media?

// Método "filter" de Dataset

winesdf.filter(col("fixed_acidity") > mean_fixed_acidity).count

// 17. Media de fixed_acidity en blancos y tintos? Recoger los datos en un Map[String, Double]

// Método "groupBy" y "agg" de Dataset
// Método "rdd" de Dataset
// Método "getAs" de Row
// Método "collectAsMap" de RDD

val mean_fixed_acidity_map = winesdf.groupBy("style").agg(mean("fixed_acidity").alias("mean_fixed_acidity")).rdd.map(r => (r.getAs[String]("style"), r.getAs[Double]("mean_fixed_acidity"))).collectAsMap()

// 18. Nueva columna que recoja, para cada vino, el incremento relativo de fixed_acidity respecto al valor promedio de este atributo en su categoría (blanco/tinto)

// Método "withColumn" de Dataset
// Objeto "Window"
// https://spark.apache.org/docs/3.3.4/sql-ref-syntax-qry-select-window.html

import org.apache.spark.sql.expressions._
winesdf.withColumn("cat_avg_fixed_acidity", mean("fixed_acidity").over(Window.partitionBy("style"))).withColumn("inc_fixed_acidity", (col("fixed_acidity") - col("cat_avg_fixed_acidity"))/col("cat_avg_fixed_acidity")).select("cat_avg_fixed_acidity", "fixed_acidity", "inc_fixed_acidity").show

// 19. En cada categoría, blancos y tintos, cuántos vinos hay con "fixed_acidity" superior al valor medio de esta variable en su categoría?

// Método "filter" y "count" de Dataset

// Objeto "Window"

winesdf.filter((col("style")==="red") && (col("fixed_acidity") > mean_fixed_acidity_map("red"))).count

winesdf.filter((col("style")==="white") && (col("fixed_acidity") > mean_fixed_acidity_map("white"))).count

winesdf.withColumn("mean_fixed_acidity_style", mean("fixed_acidity").over(Window.partitionBy("style"))).groupBy("style").agg(sum(when(col("fixed_acidity") > col("mean_fixed_acidity_style"), 1.0).otherwise(0.0)).alias("num_acid_wines")).show

// 20. Crear una columna flag ("alcohol_label") que identifique tres tipos de vino de acuerdo a su grado de alcohol: low -> (-inf, 9.5), high -> [12.0, inf), medium -> [9.5, 12)

// Método "withColumn" de Dataset
// Método "when" de functions

val alclabelf = winesdf.withColumn("alcohol_label", lit("low")).withColumn("alcohol_label", when(col("alcohol") >= 9.5 && col("alcohol") < 12, "medium").otherwise(col("alcohol_label"))).withColumn("alcohol_label", when(col("alcohol") >= 12, "high").otherwise(col("alcohol_label")))

alclabelf.show

// 21. Promedio de cada una de las variables (numéricas) por cada etiqueta de alcohol?

// Método "groupBy" y "agg" de Dataset

val mean_double_cols = double_cols.map(c => mean(c).alias("mean_" + c))

alclabelf.groupBy("alcohol_label").agg(mean_double_cols.head, mean_double_cols.tail:_*).show

// 22. Promedio y std de cada una de las variables (numéricas) por cada etiqueta de alcohol? Redondeamos a cifras con 4 decimales

// Método "groupBy" y "agg" de Dataset

// Método "round" de functions

val mean_std_double_cols = double_cols.flatMap(c => List(round(mean(c), 4).alias("mean_" + c), round(stddev(c), 4).alias("std_" + c)))

alclabelf.groupBy("alcohol_label").agg(mean_std_double_cols.head, mean_std_double_cols.tail:_*).show

// 23. Mostar en orden descendente las combinaciones de categoría y nivel de alcohol en función de pH medio

// Método "groupBy" y "agg" de Dataset
// Método "orderBy" de Dataset
// Método "desc" de functions

alclabelf.groupBy("style", "alcohol_label").agg(mean("pH").alias("mean_pH")).orderBy(desc("mean_pH")).show

// 24. Umbrales de primer y tercer cuartil para la variable "total_sulfur_dioxide"

// Método "stat" de Dataset
// Método "approxQuantile" de DataFrameStatFunctions

winesdf.stat.approxQuantile("total_sulfur_dioxide", Array(0.25, 0.75), relativeError=0.0)

// 25. Nos quedamos sólo con los vinos que no se repiten

// Objeto "Window" con partición definida por todas las columnas
// Método "withColumn" para crear una nueva columna que contenga el número de repeticiones
// Método "filter" de Dataset para cribar las filas de acuerdo al valor de la nueva columna

val cols = winesdf.columns

winesdf.withColumn("num_reps", count("*").over(Window.partitionBy(cols.head, cols.tail:_*))).filter(col("num_reps")===1)

// 26. En qué categoría, blanco o tinto, hay más vinos que no se repiten?

/// Objeto "Window" con partición definida por todas las columnas
// Método "withColumn" para crear una nueva columna que contenga el número de repeticiones
// Método when de functions para crear una nueva columna flag "norep" que indique si la fila se repite o no
// Método "groupBy" y "agg" de Dataset para contar sobre "norep"

winesdf.withColumn("num_reps", count("*").over(Window.partitionBy(cols.head, cols.tail:_*))).withColumn("norep", when(col("num_reps") === 1, 1.0).otherwise(0.0)).groupBy("style").agg(sum("norep")).show

// 27. Para cada tipo de vino, nueva columna etiqueta con el cuartil correspondiente de acuerdo al valor de pH

// Método "ntile" de functions
// Objeto "Window"

val phlabeldf = winesdf.withColumn("pH_quartile", ntile(4).over(Window.partitionBy("style").orderBy(asc("pH"))))

phlabeldf.show

// 28. Testeando la columna previa: mínimo y máximo de pH por cada tipo de vino y cuartil

// Método "groupBy" y "agg" de Dataset
// Método "min" y "max" de functions

phlabeldf.groupBy("style", "pH_quartile").agg(mean("pH").alias("pH_mean"), min("pH").alias("min_pH"), max("pH").alias("max_pH")).orderBy(asc("pH_quartile")).show

// 29. Obtener la matriz de correlaciones

// - Pares diferentes de atributos
val pairs = double_cols.toSet.subsets(2).toArray.map(_.toArray).map{case Array(f1,f2) => (f1,f2)}

// - Para cada par, calculamos la correlación. Creamos un DF

// Método "stat" de Dataset
// Método "corr" de DataFrameStatFunctions

val corr_matrix_df = pairs.map{ p =>
	val rho = winesdf.select(p._1, p._2).stat.corr(p._1, p._2)
	(p._1, p._2, rho)
}.toSeq.toDF("feat1", "feat2", "correlation")

// 30. Ordenar los pares de atributos en orden descendente de acuerdo a la correlación

// Método "orderBy" de Dataset
// Método "desc" de functions

corr_matrix_df.orderBy(desc("correlation")).show

// 31. Ordenar obviando el signo, es decir, de acuerdo a la magnitud de la correlación

// Método "withColumn" de Dataset
// Método "abs" de functions

corr_matrix_df.withColumn("abs_correlation", abs(col("correlation"))).orderBy(desc("abs_correlation")).show

// 32. Generamos un dataset artificial de tiendas de vino (500 tiendas):
// - Campos: shop_id, city, wine_id, balance
// -- shop_id: entero en [1, 20000]
// -- city: string de ["Madrid", "Sevilla", "Barcelona", "Zaragoza", "Valencia", "Valladolid", "Malaga", "Bilbao"]
// -- wine_id: one of the values in column "id" of the wine table or one unseen code
// -- balance: double de distribución normal de media 90 y desviación típica 5

val wine_id = winesiddf.select("id").distinct().rdd.map(r => r.getAs[Long]("id").toInt).collect().zipWithIndex.map{case(a,b) => (b,a)}.toMap

val num_wines = wine_id.size.toDouble

val r = scala.util.Random

val get_wine_id = udf((code: Int) => {
	if((code % 3)==0) (20000 + r.nextInt(1000)) else wine_id(code)
})

val shopdf = spark
.sqlContext
.range(499)
.withColumn("shop_id", round(lit(20000.0)*rand()).cast("int"))
.withColumn("city_code", round(lit(7.0)*rand()))
.withColumn("city", lit("none"))
.withColumn("city", when(col("city_code")===0, "Madrid").otherwise(col("city")))
.withColumn("city", when(col("city_code")===1, "Sevilla").otherwise(col("city")))
.withColumn("city", when(col("city_code")===2, "Barcelona").otherwise(col("city")))
.withColumn("city", when(col("city_code")===3, "Zaragoza").otherwise(col("city")))
.withColumn("city", when(col("city_code")===4, "Valencia").otherwise(col("city")))
.withColumn("city", when(col("city_code")===5, "Valladolid").otherwise(col("city")))
.withColumn("city", when(col("city_code")===6, "Malaga").otherwise(col("city")))
.withColumn("city", when(col("city_code")===7, "Bilbao").otherwise(col("city")))
.withColumn("id_code", round(lit(num_wines - 1.0)*rand()).cast("int"))
.withColumn("id_wine", get_wine_id(col("id_code")))
.withColumn("balance", lit(5.0)*randn() + lit(90.0))
.drop("id", "city_code", "id_code")

// 33. Número de tiendas por ciudad

// Método "groupBy" y "agg" de Dataset

shopdf.groupBy("city").agg(count("shop_id")).show

// 34. Número de tiendas diferentes por ciudad

// Método "groupBy" y "agg" de Dataset
// Método "countDistinct" de functions

shopdf.groupBy("city").agg(countDistinct("shop_id")).show

// 35. Cuántos de los vinos iniciales pueden encontrarse en las tiendas?

// Método "join" de Dataset

shopdf.select("id_wine").distinct().join(winesiddf, col("id")===col("id_wine"), "inner").count
