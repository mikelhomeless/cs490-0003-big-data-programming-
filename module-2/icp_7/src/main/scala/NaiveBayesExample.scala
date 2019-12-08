// scalastyle:off println
//package org.apache.spark.examples.mllib
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
// $example off$

object NaiveBayesExample {
  def encode(columns: Array[String], data: DataFrame): DataFrame ={
    var df = data
      for(i <- columns.indices) {
        val indexer = new StringIndexer
        df = indexer.setInputCol(columns(i))
          .setOutputCol(columns(i) + "_enc")
          .fit(df).transform(df)

        df = df.drop(columns(i))
      }

    df
  }

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    //val conf = new SparkConf().setAppName("NaiveBayesExample")
   // val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("linearRegressionWine").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // $example on$
    // Load and parse the data file.

    var data = spark.sqlContext.read.option("header", "false").csv("adult.csv")
    data = data.withColumn("_c0", data("_c0").cast(DoubleType))
      .withColumn("_c4", data("_c4").cast(DoubleType))
      .withColumn("_c10", data("_c10").cast(DoubleType))
      .withColumn("_c11", data("_c11").cast(DoubleType))
      .withColumn("_c12", data("_c12").cast(DoubleType))
      .withColumnRenamed("_c14", "label")
      .drop("_c2")

    val columns = data.columns
    val cleaned_data = encode(columns, data)

    val assembler = new VectorAssembler
    assembler.setInputCols(cleaned_data.columns.slice(0, cleaned_data.columns.length - 1))
      .setOutputCol("features")

    val x = assembler.transform(cleaned_data)

    // Split data into training (60%) and test (40%).
    val Array(train, test) = x.select(x("label_enc").alias("label"), x("features")).randomSplit(Array(0.6, 0.4))
    val evaluator = new MulticlassClassificationEvaluator

    // Naive Bayes
    val model = (new NaiveBayes).fit(train)
    val bayesPredictions = model.transform(test)
    val bayesAccuracy = evaluator.evaluate(bayesPredictions)
    println("Bayes Accuracy: " + bayesAccuracy)

    // Decision tree
    val dt = new DecisionTreeClassifier
    val dtModel = dt.setMaxBins(21648).fit(train)
    val dtPredictions = dtModel.transform(test)
    val dtAccuracy = evaluator.evaluate(dtPredictions)
    println("Decision tree accuracy: " + dtAccuracy)

    // Random Forest
    val rf = new RandomForestClassifier
    val rfModel = rf.setMaxBins(21648).fit(train)
    val rfPredictions = rfModel.transform(test)
    val rfAccuracy = evaluator.evaluate(rfPredictions)
    println("Random Forest accuracy: " + rfAccuracy)
    sc.stop()
  }
}

// scalastyle:on println