
val winesdf = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("/home/bigdata/winequality-red-white.txt").na.drop

val alclabelf = winesdf.withColumn("alcohol_label", lit("low")).withColumn("alcohol_label", when(col("alcohol") >= 9.5 && col("alcohol") < 12, "medium").otherwise(col("alcohol_label"))).withColumn("alcohol_label", when(col("alcohol") >= 12, "high").otherwise(col("alcohol_label")))

val double_cols = winesdf.dtypes.filter{case(c, t) => t.equalsIgnoreCase("DoubleType")}.map(_._1)


// 1. Aplicar la siguiente regla para identificar valores candidatos a "outliers" por cada variable
// Podremos marcar como puntos anómalos aquellos que tengan la mayoría de sus dimensiones marcadas como outliers

// 1.a) Candidato a outlier variable por variable usando Interquartile Range:

// Regla para la identificación de outiers (IQR = Q3 - Q1):
// -- Cualquier valor por debajo de Q1 – 1.5*IQR
// -- Cualquier valor por encima de Q3 + 1.5*IQR

// Generamos una nueva columna por cada variable de nombre "variable_outlier"


/*approxQuantile:
Calculates the approximate quantiles of a numerical column of a DataFrame.
The result of this algorithm has the following deterministic bound: If the DataFrame has N elements and if we request the quantile at probability p up to error err,
then the algorithm will return a sample x from the DataFrame so that the *exact* rank of x is close to (p * N). More precisely,

   floor((p - err) * N) <= rank(x) <= ceil((p + err) * N)

Params:
col – the name of the numerical column
probabilities – a list of quantile probabilities Each number must belong to [0, 1]. For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
relativeError – The relative target precision to achieve (greater than or equal to 0). If set to zero, the exact quantiles are computed, which could be very expensive.
				Note that values greater than 1 are accepted but give the same result as 1.
Returns:
the approximate quantiles at the given probabilities
Note:
null and NaN values will be removed from the numerical column before calculation. If the dataframe is empty or the column only contains null or NaN, an empty array is returned.
*/
val out_winesdf = double_cols.foldLeft(winesdf){
	case (acc, c) =>
		val Array(q1, q3) = acc.stat.approxQuantile(col = c, probabilities = Array(0.25, 0.75), relativeError = 0.0)
		acc.withColumn(c + "_outlier", when((col(c) < (lit(q1) - lit(1.5)*(lit(q3) - lit(q1)))) || (col(c) > (lit(q3) + lit(1.5)*(lit(q3) - lit(q1)))), 1).otherwise(0))
}

out_winesdf.show

// 1.b) Por cada muestra, obtenemos el número de dimensiones en las que toma valores anómalos: nos quedamos con aquellas que tengan 3 o más

out_winesdf.withColumn("num_outliers", double_cols.map(c => col(c + "_outlier")).reduce(_+_)).groupBy("num_outliers").agg(count("*")).show

val clean_winesdf = out_winesdf.withColumn("num_outliers", double_cols.map(c => col(c + "_outlier")).reduce(_+_)).filter(col("num_outliers") < 3)

clean_winesdf.show

////////////////////////////////////////////////////////////////////////////////////////

// 2. Codificación de variables categóricas ("style")

// 2.1. StringIndexer: crear una columna "idx_style"

/*
A label indexer that maps string column(s) of labels to ML column(s) of label indices.
If the input columns are numeric, we cast them to string and index the string values.
The indices are in [0, numLabels).
By default, this is ordered by label frequencies so the most frequent label gets index 0.
The ordering behavior is controlled by setting stringOrderType.
*/
import org.apache.spark.ml.feature.StringIndexer
val idxr = new StringIndexer().setInputCol("alcohol_label").setOutputCol("idx_alcohol_label").fit(alclabelf)

val idxrdf = idxr.transform(alclabelf)

idxrdf.show

idxrdf.select("alcohol_label", "idx_alcohol_label").distinct.show


// 2.2. OneHotEncoder: codificamos la columna "idx_style" indexada previamente

/*
A one-hot encoder that maps a column of category indices to a column of binary vectors, with at most a single one-value per row that indicates the input category index.
For example with 5 categories, an input value of 2.0 would map to an output vector of [0.0, 0.0, 1.0, 0.0].
The last category is not included by default (configurable via dropLast), because it makes the vector entries sum up to one, and hence linearly dependent.
So an input value of 4.0 maps to [0.0, 0.0, 0.0, 0.0].
Note:
This is different from scikit-learn's OneHotEncoder, which keeps all categories. The output vectors are sparse.
When handleInvalid is configured to 'keep', an extra "category" indicating invalid values is added as last category. So when dropLast is true, invalid values are encoded as all-zeros vector.
Note:
When encoding multi-column by using inputCols and outputCols params, input/output cols come in pairs, specified by the order in the arrays, and each pair is treated independently.
See also:
StringIndexer for converting categorical values into category indices
*/

import org.apache.spark.ml.feature.OneHotEncoder
val encdr = new OneHotEncoder().setInputCol("idx_alcohol_label").setOutputCol("enc_alcohol_label").fit(idxrdf)
//val encdr = new OneHotEncoder().setInputCol("idx_alcohol_label").setOutputCol("enc_alcohol_label").setDropLast(true).fit(idxrdf)
val encdrdf = encdr.transform(idxrdf)

//enc_alcohol_label: Vector(size: Int, indices: Array[Int], values: Array[Double])

encdrdf.select("alcohol_label", "idx_alcohol_label", "enc_alcohol_label").distinct.show

import org.apache.spark.ml.linalg._
encdrdf.select("alcohol_label", "idx_alcohol_label", "enc_alcohol_label").distinct.collect.head.getAs[SparseVector](2).toArray
encdrdf.select("alcohol_label", "idx_alcohol_label", "enc_alcohol_label").distinct.collect.last.getAs[SparseVector](2).toArray

//////////////////////////////////////////////////////////////////////////////////////

// 3. Obtener una nueva columna con un vector que incluya, como elementos, los bits de la codificación previa y la columna "fixed_acidity"

/* VectorAssembler:
A feature transformer that merges multiple columns into a vector column.
This requires one pass over the entire dataset.
In case we need to infer column lengths from the data we require an additional call to the 'first' Dataset method, see 'handleInvalid' parameter.
*/

import org.apache.spark.ml.feature._
val vAssembler = new VectorAssembler().setInputCols(Array("fixed_acidity", "enc_alcohol_label")).setOutputCol("myvector")
vAssembler.transform(encdrdf).show
vAssembler.transform(encdrdf).select("alcohol_label", "idx_alcohol_label", "enc_alcohol_label", "fixed_acidity", "myvector").filter(col("alcohol_label")==="high").show

////////////////////////////////////////////////////////////////////////////////////////

// 4. Obtener un nuevo DF a partir de clean_winesdf con la column "fixed_acidity" mapeada en el intervalo [-10, 10]

val assembler = new VectorAssembler().setInputCols(Array("fixed_acidity")).setOutputCol("vec_fixed_acidity")

val assemblerdf = assembler.transform(clean_winesdf)

assemblerdf.select("fixed_acidity", "vec_fixed_acidity").show

/* MinMaxScaler
Rescale each feature individually to a common range [min, max] linearly using column summary statistics, which is also known as min-max normalization or Rescaling.
The rescaled value for feature E is calculated as:

$$ Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (max - min) + min $$
For the case \(E_{max} == E_{min}\), \(Rescaled(e_i) = 0.5 * (max + min)\).
Note:
Since zero values will probably be transformed to non-zero values, output of the transformer will be DenseVector even for sparse input.
*/

import org.apache.spark.ml.feature.MinMaxScaler
val min_max_scaler = new MinMaxScaler().setInputCol("vec_fixed_acidity").setOutputCol("scaled_fixed_acidity").setMin(-10).setMax(10).fit(assemblerdf)

val scalerdf = min_max_scaler.transform(assemblerdf)

scalerdf.select("fixed_acidity", "scaled_fixed_acidity").show


////////////////////////////////////////////////////////////////////////////////////////

// 5. Obtener un nuevo DF con la column "perc_outliers" con el porcentaje de outliers respecto a las features (columnas de tipo double)

double_cols.size

/*
Implements the transformations which are defined by SQL statement. Currently we only support SQL syntax like 'SELECT ... FROM THIS ...' where 'THIS'
represents the underlying table of the input dataset. The select clause specifies the fields, constants, and expressions to display in the output,
it can be any select clause that Spark SQL supports. Users can also use Spark SQL built-in function and UDFs to operate on these selected columns.
For example, SQLTransformer supports statements like:
  SELECT a, a + b AS a_b FROM __THIS__
  SELECT a, SQRT(b) AS b_sqrt FROM __THIS__ where a > 5
  SELECT a, b, SUM(c) AS c_sum FROM __THIS__ GROUP BY a, b
*/
import org.apache.spark.ml.feature.SQLTransformer
val sqlTrans = new SQLTransformer().setStatement("SELECT *, (num_outliers/11) AS perc_outliers FROM __THIS__")

val transformerdf = sqlTrans.transform(scalerdf)
transformerdf.select("num_outliers", "perc_outliers").show()


////////////////////////////////////////////////////////////////////////////////////////

// 6. Construcción de un modelo de clustering a partir de clean_winesdf

// 6.a) Assembler: ensamblado de variables en una única columna

import org.apache.spark.ml.feature.VectorAssembler
val assembler = new VectorAssembler().setInputCols(double_cols).setOutputCol("features")

assembler.transform(clean_winesdf).select("features").show(10, false)

// 6.b) Scaler: normalizamos nuestras variables para que tengan desviación típica la unidad y media cero

/*
Standardizes features by removing the mean and scaling to unit variance using column summary statistics on the samples in the training set.
The "unit std" is computed using the corrected sample standard deviation, which is computed as the square root of the unbiased sample variance.
*/

import org.apache.spark.ml.feature.StandardScaler
val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaled_features").setWithStd(true).setWithMean(true)

val fitted_scaler = scaler.fit(assembler.transform(clean_winesdf))

fitted_scaler.transform(assembler.transform(clean_winesdf)).select("features", "scaled_features").show(10, false)

// 6.c) Clustering: modelo basado en k-means

import org.apache.spark.ml.clustering.KMeans
val km_model = new KMeans().setFeaturesCol("scaled_features").setPredictionCol("segment").setK(3).setMaxIter(50)

// 6.d) Construcción del pipeline

import org.apache.spark.ml.Pipeline
val pipeline = new Pipeline().setStages(Array(assembler, scaler, km_model))

// 6.e) Fit: obtenemos el transformador

val model = pipeline.fit(clean_winesdf)

// 6.f) Transform: aplicamos el transformador

val predsdf = model.transform(clean_winesdf)

// 6.g) Descripción estadística de las variables en cada cluster

val col_aggs = double_cols.map(c=>mean(c).alias("avg_" + c))
predsdf.groupBy("segment").agg(col_aggs.head, col_aggs.tail:_*).show

// 6.h) Contando el número de vinos de cada tipo (tinto/blanco) en cada segmento
predsdf.groupBy("segment", "style").agg(count("*")).orderBy("segment").show

// - segmento 2: blancos
// - segmento 0: blancos
// - segmento 1: rojos
