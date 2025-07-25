package com.sagarshingare

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator

object spark_FhvTripPipeline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC FHV Trip Analysis")
      .master("local[*]")
      .getOrCreate()

    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("data_set/nyc_taxi/fhvhv_tripdata_2025-05.parquet")

    print(rawDF.printSchema())
    val cleanedDF = rawDF
      .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
      .withColumn("dropOff_datetime", to_timestamp(col("dropOff_datetime")))
      .withColumn("trip_duration_minutes",
        (unix_timestamp(col("dropOff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60)
      .filter(col("trip_duration_minutes") > 1 && col("trip_duration_minutes") < 240)
      .na.drop(Seq("pickup_datetime", "dropOff_datetime", "PUlocationID", "DOlocationID"))

    //@TODO: Fix error : )Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/spark/ml/feature/StringIndexer
    val indexer1 = new StringIndexer().setInputCol("PUlocationID").setOutputCol("PU_loc_index")
    val indexer2 = new StringIndexer().setInputCol("DOlocationID").setOutputCol("DO_loc_index")

    val assembler = new VectorAssembler()
      .setInputCols(Array("PU_loc_index", "DO_loc_index"))
      .setOutputCol("features")

    val pipeline = new Pipeline().setStages(Array(indexer1, indexer2, assembler))
    val featureDF = pipeline.fit(cleanedDF).transform(cleanedDF)
      .select("features", "trip_duration_minutes")

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("trip_duration_minutes")

    val model = lr.fit(featureDF)
    val predictions = model.transform(featureDF)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("trip_duration_minutes")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE): $rmse")

    spark.stop()
  }
}
