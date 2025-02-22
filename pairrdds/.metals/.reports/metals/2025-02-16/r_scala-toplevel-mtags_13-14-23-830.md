error id: file://<WORKSPACE>/src/main/scala/com/example/pairrdds/CountingApp.scala:[238..244) in Input.VirtualFile("file://<WORKSPACE>/src/main/scala/com/example/pairrdds/CountingApp.scala", "package com.example.pairRdds

import org.apache.spark.{SparkConf, SparkContext}


object 

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object sparkApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

  Runner.run(conf, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile, outputFile)
}

object Runner {
  def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(inputFile)
    val counts = WordCount.withStopWordsFiltered(rdd)
    counts.saveAsTextFile(outputFile)
  }
}
")
file://<WORKSPACE>/file:<WORKSPACE>/src/main/scala/com/example/pairrdds/CountingApp.scala
file://<WORKSPACE>/src/main/scala/com/example/pairrdds/CountingApp.scala:13: error: expected identifier; obtained object
object sparkApp extends App{
^
#### Short summary: 

expected identifier; obtained object