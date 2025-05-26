import org.apache.spark.ml._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.feature._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

  def loadVoiceData(spark: SparkSession): Dataset[Row] = {

    // https://www.kaggle.com/datasets/primaryobjects/voicegender
    // wget -P /home/bigdata/Descargas/ https://gist.githubusercontent.com/mafernandez-stratio/9420a84249cc34daa85af3ac085d1a92/raw/6a4289a3c21d83d9d64016c9b82e33287d835080/voice.csv
    // Loading the data
    val voicedf = spark
    .read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/bigdata/Descargas/voice.csv")

    // Data inspection. Showing the dataset

    println("\n[Info MyVoiceDetectionApp] Dataset with " + voicedf.count + " loaded")

    voicedf

  }

  def exploreVoiceData(df: Dataset[Row]): Unit = {

    // Schema

    println("\n[Info MyVoiceDetectionApp] Structure")

    df.printSchema

    // Data inspection. Counting the number of samples in each class

    println("\n[Info MyVoiceDetectionApp] Samples per category")

    df.groupBy("label").agg(count("*")).show

    // Data inspection. NAs. Getting the columns of the df and counting the number of NA's in each of them

    println("\n[Info MyVoiceDetectionApp] Nulls per variable")

    val null_counts = df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c + "_nulls"))

    df.agg(null_counts.head, null_counts.tail:_*).show

    // Data inspection. Statistical check

    println("\n[Info MyVoiceDetectionApp] Statistics")

    df.describe().show()

  }

  def getSets(df: Dataset[Row]): (Dataset[Row], Dataset[Row]) = {

    // Creating training and test sets

    val Array(trdf, ttdf) = df.randomSplit(Array(0.5, 0.5))

    (trdf, ttdf)

  }

  def evalModelWithSomeFeats(feats: List[String], tr: Dataset[Row], tt: Dataset[Row]): Double = {
    // Defining the vector of input feats
    val assembler = new VectorAssembler()
      .setInputCols(feats.toArray)
      .setOutputCol("features")

    // Defining the label/target column
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIdx")

      /*
setNumTrees(50): Se establece el número de árboles en el bosque aleatorio en 50. Esto determina cuántos árboles de decisión se utilizarán en el modelo.
setFeatureSubsetStrategy("sqrt"): Estrategia para la selección de características en cada división de un árbol. En este caso, se utiliza "sqrt", lo que significa que se selecciona la raíz cuadrada del número de características en cada división.
setImpurity("gini"): Criterio de impureza utilizado para medir la calidad de una división en un nodo. En este caso, se utiliza "gini" como medida de impureza.
setMaxDepth(10): La profundidad máxima permitida para los árboles de decisión. Aquí se establece en 10, lo que limita la profundidad de cada árbol.
setMaxBins(32): El número máximo de bins (contenedores) a usar al dividir las características categóricas. Aquí se establece en 32.
setLabelCol("labelIdx"): Se especifica el nombre de la columna que contiene las etiquetas (clases) en el conjunto de datos. En este caso, se establece en "labelIdx".
setFeaturesCol("features"): Se especifica el nombre de la columna que contiene las características (variables predictoras) en el conjunto de datos. En este caso, se establece en "features".
setSubsamplingRate(0.7): Tasa de submuestreo para entrenamiento de árboles. Esto controla la cantidad de datos utilizados para entrenar cada árbol. Aquí se establece en 0.7, lo que significa que el 70% de los datos se utilizarán para entrenar cada árbol.
setMinInstancesPerNode(10): El número mínimo de instancias necesarias en un nodo para que se realice una división. Aquí se establece en 10.
      */

    // A random forest as classifier
    val classstage = new RandomForestClassifier()
      .setNumTrees(50)
      .setFeatureSubsetStrategy("sqrt")
      .setImpurity("gini")
      .setMaxDepth(10)
      .setMaxBins(32)
      .setLabelCol("labelIdx")
      .setFeaturesCol("features")
      .setSubsamplingRate(0.7)
      .setMinInstancesPerNode(10)

    // Defining the pipeline
    val pipeline = new Pipeline().setStages(Array(assembler, labelIndexer, classstage))

    // Training
    val model = pipeline.fit(tr)

    // Making preds on test data
    val ttpreds = model.transform(tt)

    val trpreds = model.transform(tr)

    // Evaluating the model

    // Model evaluator

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("labelIdx")
      .setRawPredictionCol("rawPrediction")

    val ttauc = evaluator.evaluate(ttpreds)

    val trauc = evaluator.evaluate(trpreds)

    println("\n[Info MyVoiceDetectionApp] Model performance - AUC(tr) = " + trauc + " - AUC(tt) = " + ttauc + "\n")

    ttauc

  }


val df = loadVoiceData(spark)

exploreVoiceData(df)

val (trainingSet, testSet) = getSets(df)

/*
  meanfreq: mean frequency (in kHz)
  sd: standard deviation of frequency
  median: median frequency (in kHz)
  Q25: first quantile (in kHz)
  Q75: third quantile (in kHz)
  IQR: interquantile range (in kHz)
  skew: skewness
  kurt: kurtosis
  sp.ent: spectral entropy
  sfm: spectral flatness
  mode: mode frequency
  centroid: frequency centroid
  peakf: peak frequency (frequency with highest energy)
  meanfun: average of fundamental frequency measured across acoustic signal
  minfun: minimum fundamental frequency measured across acoustic signal
  maxfun: maximum fundamental frequency measured across acoustic signal
  meandom: average of dominant frequency measured across acoustic signal
  mindom: minimum of dominant frequency measured across acoustic signal
  maxdom: maximum of dominant frequency measured across acoustic signal
  dfrange: range of dominant frequency measured across acoustic signal
  modindx: modulation index. Calculated as the accumulated absolute difference between adjacent measurements of fundamental frequencies divided by the frequency range
  label: male or female
*/

val allFeats = List("meanfreq", "sd", "median", "Q25", "Q75", "IQR", "skew", "kurt", "spent", "sfm", "mode", "centroid", "meanfun", "minfun", "maxfun", "meandom", "mindom", "maxdom", "dfrange", "modindx")

val allEvalsWithTake = Range(1, 21).map(n => (evalModelWithSomeFeats(allFeats.take(n), trainingSet, testSet), n))
allEvalsWithTake.max

val allEvalsWithSlice = Range(0, 20).map(n => (evalModelWithSomeFeats(allFeats.slice(n, 20), trainingSet, testSet), n))
allEvalsWithSlice.max
