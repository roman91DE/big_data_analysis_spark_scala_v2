package com.example.pwlist

import org.apache.spark.{SparkConf, SparkContext}


object CountingLocalApp extends App{
  val inputFile = "data/pw-list.txt"
  val nGram = 10
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")
  Runner.run(conf, inputFile, nGram)
}



object Runner {
  def run(conf: SparkConf, inputFile: String, ngram: Int): Unit = {
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rddPW = sc.textFile(inputFile)
    // val rddChars = rddPW.flatMap(_.toLowerCase.toList)
    val ngramRDD = rddPW.flatMap { word =>
      val text = word.toLowerCase.replaceAll("[^a-z]", "")
      if (text.length < ngram) {
        Seq.empty[String] 
      } else {
        text.sliding(ngram).toSeq
      }
    }
    val rddKV = ngramRDD.map(c => (c, 1))
    val result = rddKV.reduceByKey((l, r) => l+r).sortBy(_._2, false)
    result.take(50).foreach(println)
  }
}
