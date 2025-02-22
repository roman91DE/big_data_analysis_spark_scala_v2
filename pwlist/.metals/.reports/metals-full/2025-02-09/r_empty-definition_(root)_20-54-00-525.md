error id: 
file://<WORKSPACE>/src/main/scala/com/example/pwlist/CountingApp.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
|empty definition using fallback
non-local guesses:
	 -wordsRDD.
	 -scala/Predef.wordsRDD.

Document text:

```scala
package com.example.pwlist

import org.apache.spark.{SparkConf, SparkContext}


object CountingLocalApp extends App{
  val inputFile = "data/pw-list.txt"
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

  Runner.run(conf, inputFile)
}



object Runner {
  def run(conf: SparkConf, inputFile: String): Unit = {
    val sc = new SparkContext(conf)
    val rddPW = sc.textFile(inputFile)
    // val rddChars = rddPW.flatMap(_.toLowerCase.toList)
    val trigramsRDD = wordsRDD.flatMap { word =>
      val chars = word.toLowerCase.replaceAll("[^a-z]", "") // Keep only alphabetic characters
      if (chars.length < 3) {
        Seq.empty[String] // No trigrams possible for words shorter than 3 characters
      } else {
        chars.sliding(3).toSeq
      }
    }


    val rddKV = rddChars.map(c => (c, 1))

    val result = rddKV.reduceByKey((l, r) => l+r).sortBy(_._2, false)
    result.take(10).foreach(println)
  }
}

```

#### Short summary: 

empty definition using pc, found symbol in pc: 