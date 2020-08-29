package org.spark

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object SparkCombiner {

  def rddGroupByKey(sc: SparkContext, input: String, output: String, logger: Logger): Unit = {
    val inputRDD = sc.textFile(input)
    val pairsRDD = inputRDD.map(row => (row.split(",")(1), 1))
    val groupedRDD = pairsRDD.groupByKey()
    logger.info(groupedRDD.toDebugString)
    val followersCountRDD = groupedRDD.map(x => (x._1, x._2.sum))
    followersCountRDD.saveAsTextFile(output)
  }

  def rddReduceByKey(sc: SparkContext, input: String, output: String, logger: Logger): Unit = {
    val inputRDD = sc.textFile(input)
    val pairsRDD = inputRDD.map(row => (row, 1))
    val reducedRDD = pairsRDD.reduceByKey(_+_)
    logger.info(reducedRDD.toDebugString)
    reducedRDD.saveAsTextFile(output)
  }

  def rddFoldByKey(sc: SparkContext, input: String, output: String, logger: Logger): Unit = {
    val inputRDD = sc.textFile(input)
    val pairsRDD = inputRDD.map(row => (row.split(",")(1), 1))
    val foldRDD = pairsRDD.foldByKey(0)(_+_)
    logger.info(foldRDD.toDebugString)
    foldRDD.saveAsTextFile(output)
  }

  def rddAggregateByKey(sc: SparkContext, input: String, output: String, logger: Logger): Unit = {
    val inputRDD = sc.textFile(input)
    val pairsRDD = inputRDD.map(row => (row.split(",")(1), 1))
    val aggregateRDD = pairsRDD.aggregateByKey(0)(_+_,_+_)
    logger.info(aggregateRDD.toDebugString)
    aggregateRDD.saveAsTextFile(output)
  }

  def groupByDataset(spark: SparkSession, input: String, output: String, logger: Logger): Unit = {
    val dataSchema = StructType(Array(
      StructField("follower", LongType, nullable = false),
      StructField("user", LongType, nullable = false)))
    val inputDF = spark.read.format("csv").schema(dataSchema).load(input)
    val groupDF = inputDF.groupBy("user").count()
    logger.info(groupDF.explain(extended = true))
    groupDF.write.csv(output)
  }

  def main(args : Array[String]): Unit = {
    val logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\norg.spark.SparkCombiner <input dir> <output dir>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder()
      .appName("Spark Twitter")
      .config("spark.logLineage", "true")
      .getOrCreate()

    val sc = spark.sparkContext

    rddGroupByKey(sc, args(0), args(1), logger)
    //rddReduceByKey(sc, args(0), args(1), logger)
    //rddFoldByKey(sc, args(0), args(1), logger)
    //rddAggregateByKey(sc, args(0), args(1), logger)
    //groupByDataset(spark, args(0), args(1), logger)
  }
}
