package org.spark

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.functions.broadcast

object SparkJoins {

  val MAX_FILTER = 15000;

  def rddReduceSideJoin(sc: SparkContext, input: String, output: String, logger: Logger): Unit =  {
    val rowRDD = sc.textFile(input)
    val filterRDD = rowRDD.filter(row => {
      val rowVals = row.split(",")
      val from = rowVals(0)
      val to = rowVals(1)
      from.toLong < MAX_FILTER && to.toLong < MAX_FILTER
    })
    val fromRDD = filterRDD.map(row => {
      val rowVals = row.split(",")
      val from = rowVals(0).toLong
      val to = rowVals(1).toLong
      (from, to)
    })
    val toRDD = filterRDD.map(row => {
      val rowVals = row.split(",")
      val from = rowVals(0).toLong
      val to = rowVals(1).toLong
      (to, from)
    })
    val counter = sc.longAccumulator("Triangle Accumulator")
    val path2RDD = joinOnKey(fromRDD, toRDD).map(_._2)
    val triangleRDD = joinOnKey(path2RDD, fromRDD).map(_._2)
    triangleRDD.foreach { x => if (x._1 == x._2) counter.add(1) }
    logger.info("RS-R Triangle Count = " + counter.value / 3)
  }

  def joinOnKey(fromRDD: RDD[(Long, Long)],
                toRDD: RDD[(Long, Long)]): RDD[(Long, (Long, Long))] = {
    val joinedRDD = fromRDD.join(toRDD)
    joinedRDD
  }

  def dsReduceSideJoin(spark: SparkSession, input: String, output: String, logger: Logger): Unit = {
    import spark.implicits._

    val customSchema = StructType(Array(
      StructField("follower_id", LongType, nullable = false),
      StructField("followee_id", LongType, nullable = false)
    ))
    val followerDS = spark.read.format("csv").schema(customSchema)
      .load(input)
      .where($"follower_id" < MAX_FILTER && $"followee_id" < MAX_FILTER)

    val path2DS: Dataset[(Row, Row)] =
      followerDS.as("a").joinWith(followerDS.as("b"),
        $"a.followee_id" === $"b.follower_id")

    val triangleDS: Dataset[((Row, Row), Row)] =
      path2DS.as("a").joinWith(followerDS.as("b"),
        $"a._1.follower_id" === $"b.followee_id" && $"a._2.followee_id" === $"b.follower_id")

    logger.info("RS-D Triangle Count = " + triangleDS.count() / 3)
  }

  def rddReplicatedJoin(sc: SparkContext, input: String, output: String, logger: Logger): Unit = {
    val rowRDD = sc.textFile(input)
    val filterRDD = rowRDD.filter(row => {
      val rowValues = row.split(",")
      val from = rowValues(0)
      val to = rowValues(1)
      from.toLong < MAX_FILTER && to.toLong < MAX_FILTER
    })
    val fromRDD = filterRDD.map(row => {
      val rowValues = row.split(",")
      val from = rowValues(0)
      val to = rowValues(1)
      (from, to)
    })
    val toRDD = filterRDD.map(row => {
      val rowValues = row.split(",")
      val from = rowValues(0)
      val to = rowValues(1)
      (from, to)
    })
    val counter = sc.longAccumulator("Triangle Accumulator")
    val edgesRDD = fromRDD.collect().groupBy { case (from, to) => to }
    val broadcastVal = sc.broadcast(edgesRDD)
    val path2 = toRDD.mapPartitions(iter => {
      iter map {case (from, to) =>
        if (broadcastVal.value.get(from).isDefined) {
          val arr: Array[(String, String)] = broadcastVal.value(from)
          arr.foreach { case (mapFrom, mapTo) =>
            if (broadcastVal.value.get(mapFrom).isDefined) {
              val arr1: Array[(String, String)] = broadcastVal.value(mapFrom)
              arr1.foreach { case (f, t) =>
                if (to.equals(f)) {
                  counter.add(1)
                }
              }
            }
          }
        }
      }
    }, preservesPartitioning = true)
    logger.info("Rep-R Triangle Count = " + counter.value / 3)

  }

  def dsReplicatedJoin(spark: SparkSession, input: String, output: String, logger: Logger): Unit = {
    import spark.implicits._

    val customSchema = StructType(Array(
      StructField("follower_id", LongType, nullable = false),
      StructField("followee_id", LongType, nullable = false)))

    val followersDS = spark.read.format("csv").schema(customSchema).
      load(input)
      .where($"follower_id" < MAX_FILTER && $"followee_id" < MAX_FILTER)

    val path2DS: DataFrame =
      followersDS.as("a").join(broadcast(followersDS).as("b"),
        $"a.followee_id" === $"b.follower_id").select($"a.follower_id", $"b.followee_id")

    val triangleDS: Dataset[(Row, Row)] =
      path2DS.as("a").joinWith(followersDS.as("b"),
        $"a.follower_id" === $"b.followee_id" && $"a.followee_id" === $"b.follower_id")

    logger.info("Rep-D Triangle Count = " + triangleDS.count() / 3)

  }

  def main(args : Array[String]): Unit = {
    val logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\norg.spark.SparkJoin <input dir> <output dir>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder()
      .appName("Spark Twitter")
      .config("spark.logLineage", "true")
      .getOrCreate()

    val sc = spark.sparkContext

    rddReduceSideJoin(sc, args(0), args(1), logger)
    //dsReduceSideJoin(spark, args(0), args(1), logger)
    //rddReplicatedJoin(sc, args(0), args(1), logger)
    //dsReplicatedJoin(spark, args(0), args(1), logger)
  }
}
