error id: file://<WORKSPACE>/src/main/scala/com/example/pairrdds/SparkApp.scala:[240..246) in Input.VirtualFile("file://<WORKSPACE>/src/main/scala/com/example/pairrdds/SparkApp.scala", "package com.example.pairRdds

import org.apache.spark.{SparkConf, SparkContext}


object Spark {
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

  val context = new SparkContext(conf)
}

case class 

object Data {

}

object SparkApp extends App{
  val spark = Spark.context
}


")
file://<WORKSPACE>/file:<WORKSPACE>/src/main/scala/com/example/pairrdds/SparkApp.scala
file://<WORKSPACE>/src/main/scala/com/example/pairrdds/SparkApp.scala:16: error: expected identifier; obtained object
object Data {
^
#### Short summary: 

expected identifier; obtained object