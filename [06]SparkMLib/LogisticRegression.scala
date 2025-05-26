/*
https://rpubs.com/mentadoh/1065665
var_1 -> RS -> The number of runs scored by the team in that year
var_2 -> RA -> The number of runs allowed by the team in that year
var_3 -> W -> The number of regular season wins by the team in that year
var_4 -> OBP -> The on-base percentage of the team in that year
var_5 -> BA -> The batting average of the team in that year
output -> SLG -> The slugging percentage of the team in that year
*/

// wget -P /home/bigdata/Descargas/ https://gist.githubusercontent.com/mafernandez-stratio/d9171e9582013b085395938980dc0fdd/raw/dcba1a4fefaa466e2be1a48dae749cb448c52d44/Linear_regression_dataset.csv

// Linear Regression
	// import Linear Regression from spark's MLlib
import org.apache.spark.ml._
	// Load the dataset
val initialDF = spark.read.option("inferSchema", "true").option("header", "true").csv("/home/bigdata/Descargas/Linear_regression_dataset.csv")
val df = initialDF.select(col("var_1").as("rs"), col("var_2").as("ra"), col("var_3").as("w"), col("var_4").as("obp"), col("var_5").as("ba"), col("output"))
val inputCols = df.schema.toArray.take(5).map(_.name)
	// validate the size of data
println(df.count(), df.columns.length)
	// explore the data
df.printSchema
	// sneak into the dataset
df.head(5)
	// view statistical measures of data
df.describe().show(500, false)
import org.apache.spark.sql.functions._
	// check for correlation
df.select(corr("rs", "output")).show()
	// ¿Cual es la correlación entre output con las demas variables?
df.select(corr("rs","output"),corr("ra","output"),corr("w","output"),corr("obp","output"),corr("ba","output")).show()
inputCols.foreach(colName => df.select(corr(colName, "output")).show())
df.select(corr("rs","obp")).show()
df.select(corr("rs","ba")).show()
	// import vectorassembler to create dense vectors
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.feature._
	// select the columns to create input vector
df.columns

// Code for expected result...

scaledData.show(5, false)

// Expected result:
/*
+---+---+---+-----+-----+------+------------------------------+---------------------------------------------------------------------------------------------+
|rs |ra |w  |obp  |ba   |output|NonscaledFeatures             |features                                                                                     |
+---+---+---+-----+-----+------+------------------------------+---------------------------------------------------------------------------------------------+
|734|688|81 |0.328|0.259|0.418 |[734.0,688.0,81.0,0.328,0.259]|[8.018852471248053,7.3914965401417625,7.06921077206201,21.848063282324997,20.066274600440245]|
|700|600|94 |0.32 |0.247|0.389 |[700.0,600.0,94.0,0.32,0.247] |[7.64740698892866,6.446072564077118,8.20377546387443,21.31518369007317,19.136563035941084]   |
|712|705|93 |0.311|0.247|0.417 |[712.0,705.0,93.0,0.311,0.247]|[7.778505394453152,7.574135262790614,8.116501256811937,20.71569414878986,19.136563035941084] |
|734|806|69 |0.315|0.26 |0.415 |[734.0,806.0,69.0,0.315,0.26] |[8.018852471248053,8.659224144410262,6.021920287312082,20.982133944915773,20.143750564148508]|
|613|759|61 |0.302|0.24 |0.378 |[613.0,759.0,61.0,0.302,0.24] |[6.696943548876098,8.154281793557555,5.323726630812131,20.11620460750655,18.594231289983238] |
+---+---+---+-----+-----+------+------------------------------+---------------------------------------------------------------------------------------------+
*/

	// create data containing input features and output column
val model_df = scaledData.select("features", "output")
model_df.show(5, false)
	// size of model df
println(model_df.count(), model_df.columns.size)
// Split Data - Train & Test sets
	// split the data into 70/30 ratio for train test purpose
val Array(train_df, test_df): Array[DataFrame] = model_df.randomSplit(Array(0.7, 0.3))
(train_df.count(), train_df.columns.size)
(test_df.count(), test_df.columns.size)
train_df.describe().show()
test_df.describe().show()
// Build Linear Regression Model
import org.apache.spark.ml.regression._
	// Build Linear Regression model
val lin_Reg = new LinearRegression().setLabelCol("output")
	// fit the linear regression model on training data set
val lr_model: LinearRegressionModel = lin_Reg.fit(train_df)
println(lr_model.intercept)
println(lr_model.coefficients)
val training_predictions = lr_model.transform(train_df)
training_predictions.show(10, false)
println(lr_model.summary.meanSquaredError) //error cuadrático medio (MSE)
println(lr_model.summary.r2) //coeficiente de determinación
	// make predictions on test data
val test_results = lr_model.evaluate(test_df)
test_results.residuals.show(10, false)
test_results.predictions.show(10, false)
	// coefficient of determination value for model
println(test_results.r2)
println(test_results.meanSquaredError)
println(test_results.rootMeanSquaredError) //raíz del error cuadrático medio (RMSE)
