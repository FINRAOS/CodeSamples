// Databricks notebook source
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, functions}

def formatData(df: DataFrame, fields: Seq[String], continuousFieldIndexes: Seq[Int]): DataFrame = {
  var data = df

  // Trim leading spaces from data
  for (colName <- data.columns)
    data = data.withColumn(colName, functions.ltrim(functions.col(colName)))

  // Assign column names
  for (i <- fields.indices)
    data = data.withColumnRenamed("_c" + i, fields(i))

  data = data.withColumnRenamed("_c14", "label")

  // Convert continuous values from string to double
  for (i <- continuousFieldIndexes) {
    data = data.withColumn(fields(i), functions.col(fields(i)).cast("double"))
  }

  // Remove '.' character from label
  data = data.withColumn("label", functions.regexp_replace(functions.col("label"), "\\.", ""))

  data
}

def removeFields(fields: Seq[String], categoricalFieldIndexes: Seq[Int], continuousFieldIndexes: Seq[Int], removeFields: String*):
(Seq[String], Seq[Int], Seq[Int]) = {
  var fieldsUpdated = fields
  var categoricalFieldIndexesUpdated = categoricalFieldIndexes
  var continuousFieldIndexesUpdated = continuousFieldIndexes

  for (removeField <- removeFields) {
    val removeIndex = fieldsUpdated.indexOf(removeField)
    fieldsUpdated = fieldsUpdated.filter(x => !x.equals(removeField))
    categoricalFieldIndexesUpdated = updateIndex(removeIndex, categoricalFieldIndexesUpdated)
    continuousFieldIndexesUpdated = updateIndex(removeIndex, continuousFieldIndexesUpdated)
  }

  (fieldsUpdated, categoricalFieldIndexesUpdated, continuousFieldIndexesUpdated)
}

def updateIndex(removeIndex: Int, indexes: Seq[Int]): Seq[Int] = {
  indexes
    .filter(x => !x.equals(removeIndex))
    .map(x =>
      if (x > removeIndex) x - 1
      else x
    )
}

def showCategories(df: DataFrame, fields: Seq[String], categoricalFieldIndexes: Seq[Int], maxRows: Int): Unit = {
  for (i <- categoricalFieldIndexes) {
    val colName = fields(i)
    df.select(colName + "Indexed", colName).distinct().sort(colName + "Indexed").show(maxRows)
  }
}

def predictionsForPartialDependencePlot(schema: StructType, indexedData: DataFrame, testData: DataFrame, model: PipelineModel, fieldName: String): DataFrame = {
  var predictions = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  for (i <- indexedData.select(fieldName).distinct().orderBy(fieldName).collect()) {
    val testDataIteration = testData.withColumn(fieldName, functions.lit(i.get(0)))
    predictions = predictions.union(model.transform(testDataIteration))
  }

  predictions
}

def expandPredictions(predictions: DataFrame): DataFrame = {
  val vectorElem = functions.udf((x: Vector, i: Integer) => x(i))

  predictions
//    .where("indexedLabel = prediction")
    .withColumn("rawPrediction0", vectorElem(predictions.col("rawPrediction"), functions.lit(0)))
    .withColumn("rawPrediction1", vectorElem(predictions.col("rawPrediction"), functions.lit(1)))
    .withColumn("score0", vectorElem(predictions.col("probability"), functions.lit(0)))
    .withColumn("score1", vectorElem(predictions.col("probability"), functions.lit(1)))
}

// COMMAND ----------

var fields = Seq(
  "age",
  "workclass",
  "fnlwgt",
  "education",
  "education-num",
  "marital-status",
  "occupation",
  "relationship",
  "race",
  "sex",
  "capital-gain",
  "capital-loss",
  "hours-per-week",
  "native-country"
)
var categoricalFieldIndexes = Seq(1, 3, 5, 6, 7, 8, 9, 13)
var continuousFieldIndexes = Seq(0, 2, 4, 10, 11, 12)

// COMMAND ----------

// Create dataframe to hold census income training data
// Data retrieved from http://archive.ics.uci.edu/ml/datasets/Census+Income
val trainingUrl = "https://raw.githubusercontent.com/FINRAOS/CodeSamples/master/machine-learning-samples/src/main/resources/adult.data"
val trainingContent = scala.io.Source.fromURL(trainingUrl).mkString

val trainingList = trainingContent.split("\n").filter(_ != "")

val trainingDs = sc.parallelize(trainingList).toDS()
var trainingData = spark.read.csv(trainingDs).cache

// COMMAND ----------

// Create dataframe to hold census income test data
// Data retrieved from http://archive.ics.uci.edu/ml/datasets/Census+Income
val testUrl = "https://raw.githubusercontent.com/FINRAOS/CodeSamples/master/machine-learning-samples/src/main/resources/adult.test"
val testContent = scala.io.Source.fromURL(testUrl).mkString

val testList = testContent.split("\n").filter(_ != "")

val testDs = sc.parallelize(testList).toDS()
var testData = spark.read.csv(testDs).cache

// COMMAND ----------

// Format the data
trainingData = formatData(trainingData, fields, continuousFieldIndexes)
testData = formatData(testData, fields, continuousFieldIndexes)

// COMMAND ----------

// Exclude redundant and weighted attributes from feature vector
val (fieldsUpdated, categoricalFieldIndexesUpdated, continuousFieldIndexesUpdated) = removeFields(
  fields, categoricalFieldIndexes, continuousFieldIndexes, "education-num", "relationship", "fnlwgt")
fields = fieldsUpdated
categoricalFieldIndexes = categoricalFieldIndexesUpdated
continuousFieldIndexes = continuousFieldIndexesUpdated

// COMMAND ----------

import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}

// Create object to convert categorical values to index values
val categoricalIndexerArray =
  for (i <- categoricalFieldIndexes)
    yield new StringIndexer()
      .setInputCol(fields(i))
      .setOutputCol(fields(i) + "Indexed")

// Create object to index label values
val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")
  .fit(trainingData)

// Create object to generate feature vector from categorical and continuous values
val vectorAssembler = new VectorAssembler()
  .setInputCols((categoricalFieldIndexes.map(i => fields(i) + "Indexed") ++ continuousFieldIndexes.map(i => fields(i))).toArray)
  .setOutputCol("features")

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier

// Create decision tree
val dt = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("features")
  .setNumTrees(50)
  .setMaxBins(100) // Since feature "native-country" contains 42 distinct values, need to increase max bins to at least 42.
  .setMaxDepth(10)
  .setImpurity("gini")

// Create object to convert indexed labels back to actual labels for predictions
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)

// Array of stages to run in pipeline
val indexerArray = Array(labelIndexer) ++ categoricalIndexerArray
val stageArray = indexerArray ++ Array(vectorAssembler, dt, labelConverter)

val pipeline = new Pipeline()
  .setStages(stageArray)

// Train the model
val model = pipeline.fit(trainingData)

// Test the model
val predictions = model.transform(testData)

// COMMAND ----------

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println(s"Test error = ${1.0 - accuracy}\n")

val metrics = new MulticlassMetrics(
  predictions.select("indexedLabel", "prediction")
  .rdd.map(x => (x.getDouble(0), x.getDouble(1)))
)

println(s"Confusion matrix:\n ${metrics.confusionMatrix}\n")

val treeModel = model.stages(stageArray.length - 2).asInstanceOf[RandomForestClassificationModel]

val featureImportances = treeModel.featureImportances.toArray.zipWithIndex.map(x => Tuple2(fields(x._2), x._1)).sortWith(_._2 > _._2)
println("Feature importances sorted:")
featureImportances.foreach(x => println(x._1 + ": " + x._2))
println()

// Print out the tree with actual column names for features
var treeModelString = treeModel.toDebugString

val featureFieldIndexes = categoricalFieldIndexes ++ continuousFieldIndexes
for (i <- featureFieldIndexes.indices)
  treeModelString = treeModelString
    .replace("feature " + i + " ", fields(featureFieldIndexes(i)) + " ")

println(s"Learned classification tree model:\n $treeModelString")

// COMMAND ----------

for (i <- featureFieldIndexes.indices)
  println(s"feature " + i + " -> " + fields(featureFieldIndexes(i)))

// COMMAND ----------

display(treeModel)

// COMMAND ----------

display(predictions.select("label", Seq("predictedLabel" ,"indexedLabel", "prediction") ++ fields:_*))

// COMMAND ----------

val wrongPredictions = predictions
  .select("label", Seq("predictedLabel" ,"indexedLabel", "prediction") ++ fields:_*)
  .where("indexedLabel != prediction")
display(wrongPredictions)

// COMMAND ----------

// Show the label and all the categorical features mapped to indexes
val indexedData = new Pipeline()
  .setStages(indexerArray)
  .fit(trainingData)
  .transform(trainingData)
indexedData.select("indexedLabel", "label").distinct().sort("indexedLabel").show()
showCategories(indexedData, fields, categoricalFieldIndexes, 100)

// COMMAND ----------

// Partial dependence plots
val predictionsAge = predictionsForPartialDependencePlot(predictions.schema, indexedData, testData, model, "age")
val predictionsEducation = predictionsForPartialDependencePlot(predictions.schema, indexedData, testData, model, "education")
val predictionsMaritalStatus = predictionsForPartialDependencePlot(predictions.schema, indexedData, testData, model, "marital-status")

val predictionsAgeExpanded = expandPredictions(predictionsAge)
val predictionsEducationExpanded = expandPredictions(predictionsEducation)
val predictionsMaritalStatusExpanded = expandPredictions(predictionsMaritalStatus)

// COMMAND ----------

display(predictionsAgeExpanded.orderBy($"age".asc))

// COMMAND ----------

display(predictionsEducationExpanded.orderBy($"score1".asc))

// COMMAND ----------

display(predictionsMaritalStatusExpanded.orderBy($"score1".asc))
