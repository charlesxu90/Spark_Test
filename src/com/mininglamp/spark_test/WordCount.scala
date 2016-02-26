package com.mininglamp.spark_test
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by charles on 12/17/15.
  */
object WordCount {
  def main(args: Array[String]) {
    val inputFile = "/Users/admin/Documents/IdeaProjects/Spark/spark-1.5.1-bin-hadoop2.6/README.md"
    val outputFile = "/Users/admin/Downloads/tmp"
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile(inputFile)
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = {
      words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    } // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }
}