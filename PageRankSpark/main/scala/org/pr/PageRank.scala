package org.pr

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object PageRank {

  def toPairEdges(nodeList: List[Int], k: Int): List[(String, String)] = {
    nodeList.sliding(2).map(n => {
      if (n.head % k == 0) {
        (n.head.toString, "0")
      }
      else {
        (n.head.toString, n.tail.head.toString)
      }
    }).toList
  }

  def main(args: Array[String]): Unit = {
    val logger: Logger = LogManager.getRootLogger;
    if (args.length != 1) {
      logger.error("Usage:\norg.pr.PageRank <K>")
      System.exit(1)
    }
    val spark = SparkSession.builder.appName("PageRank").getOrCreate()

    val k = args(0).toInt
    val numNodes = k * k
    val nodeList = List.range(1, numNodes + 2)
    val initialPR = 1.0 / numNodes

    val edges = toPairEdges(nodeList, k)
    val ranks = List.range(0, numNodes + 1).map(r => if (r == 0) (r.toString, 0.0) else (r.toString, initialPR))

    val graphRDD = spark.sparkContext.parallelize(edges).cache()
    var rankRDD = spark.sparkContext.parallelize(ranks)

    val iterations = 10
    for (i <- 1 to iterations) {
      val joinedRDD = graphRDD.join(rankRDD)
      val tempRDD = joinedRDD.flatMap(kv =>
        if (kv._1.toInt % k == 1) {
          List((kv._1, 0.0), kv._2)
        }
        else {
          List(kv._2)
      })
      val temp2RDD = tempRDD.reduceByKey(_+_)
      val delta = temp2RDD.lookup("0").head

      rankRDD = temp2RDD.map(kv => {
        if (kv._1.equals("0")) {
          (kv._1, kv._2)
        }
        else {
          val updatedPR = kv._2 + delta/numNodes
          (kv._1, updatedPR)
        }
      })
      logger.info("PageRank sum for iteration " + i + ":" + rankRDD.filter(_._1 != "0").map(_._2).sum())
    }

    val topKRDDByPR = rankRDD.filter(_._1 != "0").takeOrdered(k)(Ordering[Double].reverse.on {r => r._2})
    logger.info("Top K Values By PR")
    logger.info(topKRDDByPR.foreach(println))

    val topKRDD = rankRDD.takeOrdered(numNodes + 1)(Ordering[Int].on {r => r._1.toInt})
    logger.info("PageRank for K Nodes")
    logger.info(topKRDD.foreach(println))

    rankRDD.saveAsTextFile("output")

    spark.stop()
  }
}