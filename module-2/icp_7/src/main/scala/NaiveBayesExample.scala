// scalastyle:off println
//package org.apache.spark.examples.mllib
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.clustering.KMeans
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

  def allToDouble(data: DataFrame): DataFrame = {
    var newFrame = data
    val columns = data.columns
    for(i <- columns.indices){
      newFrame = newFrame.withColumn(columns(i), newFrame(columns(i)).cast(DoubleType))
    }
    newFrame
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
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

    val cleanBayes = assembler.transform(cleaned_data)

    // Split data into training (60%) and test (40%).
    val Array(classTrain, classTest) = cleanBayes.withColumnRenamed("label_enc", "label").randomSplit(Array(0.6, 0.4))
    val evaluator = new MulticlassClassificationEvaluator

    // Naive Bayes
    val bayes = new NaiveBayes()
      .setFeaturesCol("features")

    val model = bayes.fit(classTrain)
    val bayesPredictions = model.transform(classTest)
    val bayesAccuracy = evaluator.evaluate(bayesPredictions)
    println("Bayes Accuracy: " + bayesAccuracy)

    // Decision tree
    val dt = new DecisionTreeClassifier
    val dtModel = dt.setMaxBins(21648).fit(classTrain)
    val dtPredictions = dtModel.transform(classTest)
    val dtAccuracy = evaluator.evaluate(dtPredictions)
    println("Decision tree accuracy: " + dtAccuracy)

    // Random Forest
    val rf = new RandomForestClassifier
    val rfModel = rf.setMaxBins(21648).fit(classTrain)
    val rfPredictions = rfModel.transform(classTest)
    val rfAccuracy = evaluator.evaluate(rfPredictions)
    println("Random Forest accuracy: " + rfAccuracy)

    // KMEANS
    val clusterData = spark.sqlContext.read.option("header", "true").csv("diabetic_data.csv")
        .withColumnRenamed("readmitted", "label")
        .drop("encounter_id")
        .drop("patient_nbr")
        .drop("weight")
        .drop("payer_code")
        .drop("medical_specialty")

    val cleaned_cluster_data = encode(clusterData.columns, clusterData)

    val clusterAssembler = new VectorAssembler
    clusterAssembler.setInputCols(cleaned_cluster_data.columns.slice(0, cleaned_cluster_data.columns.length - 1))
      .setOutputCol("features")

    val cleanCluster = clusterAssembler.transform(cleaned_cluster_data)

    // Split data into training (60%) and test (40%).
    val Array(kTrain, kTest) = cleanCluster.withColumnRenamed("label_enc", "label").randomSplit(Array(0.6, 0.4))

    val km = new KMeans()
        .setFeaturesCol("features")

    km.setK(10).setSeed(32)
    val kmModel = km.fit(kTrain)

    val kmPredictions = kmModel.transform(kTest.cache())
    val wssse= kmModel.computeCost(kmPredictions)
    println("WSSSE: " + wssse)


    // REGRESSION
    val regressionData = spark.sqlContext.read.option("header", "true").csv("imports-85.csv")
    val encodeColumns = Array("make", "fuel-type", "aspiration", "num-of-doors", "body-style", "drive-wheels", "engine-location", "engine-type", "num-of-cylinders", "fuel-system")
    var cleanedRegressionData = allToDouble(encode(encodeColumns, regressionData))

    val regressionAssembler = new VectorAssembler
    regressionAssembler.setInputCols(cleanedRegressionData.columns.slice(0, cleanedRegressionData.columns.length - 1))
      .setOutputCol("features")

    cleanedRegressionData.columns.foreach(col =>
      cleanedRegressionData = cleanedRegressionData
        .filter(cleanedRegressionData(col).isNotNull))

    val ra = regressionAssembler.transform(cleanedRegressionData)

    // Split data into training (60%) and test (40%).
    val Array(train, test) = ra.randomSplit(Array(0.6, 0.4))

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("price")

    // Linear Regression
    val lrModel = lr.fit(train)
    val lrPredictions = lrModel.transform(test)
    val evaluation_summary = lrModel.evaluate(test)

    println("LINEAR REGRESSION RESULTS...")
    lrPredictions.select("price", "prediction").show()
    println("Mean Squared Error: " + evaluation_summary.meanSquaredError + "\n\n\n")

    // Logistic regression
    val logR = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("price")

    val logRModel =logR.fit(train)
    val logrPredictions = logRModel.transform(test)
    val logrEvaluation = logRModel.evaluate(test)

    println("LOGISTIC REGRESSION RESULTS...")
    logrPredictions.select("price", "prediction").show()
    sc.stop()
  }
}

// scalastyle:on println