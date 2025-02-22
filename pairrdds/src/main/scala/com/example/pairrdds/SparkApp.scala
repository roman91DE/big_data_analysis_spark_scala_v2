package com.example.pairRdds

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object Spark {
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

  val context = new SparkContext(conf)
  context.setLogLevel("ERROR")
}

case class UserMail(id: Int, email: String)
case class UserPW(id: Int, pw: String)

object Data {
  val userMails: List[UserMail] = List(
    UserMail(1, "alice@example.com"),
    UserMail(2, "bob@example.com"),
    UserMail(3, "charlie@example.com"),
    UserMail(4, "dave@example.com"),
    UserMail(5, "eve@example.com"),
    UserMail(6, "no@password.com")
  )

  val userPasswords: List[UserPW] = List(
    UserPW(1, "pass1234"),
    UserPW(2, "securePass!"),
    UserPW(3, "qwerty123"),
    UserPW(4, "hunter2"),
    UserPW(5, "letmein")
  )
}

object SparkApp extends App{
  val spark = Spark.context

  val mailRdd: RDD[(Int, String)] = spark.parallelize(Data.userMails).map(
    (obj) => (obj.id, obj.email)
  )

  val pwRdd: RDD[(Int, String)] = spark.parallelize(Data.userPasswords).map(
    (obj) => (obj.id, obj.pw)
  )


  val joined = mailRdd.leftOuterJoin(pwRdd).sortBy(_._1)
  val pwned = joined.filter(r => !r._2._2.isEmpty)

  println("Users PWNED")
  pwned foreach println

  val save = joined.filter(r => r._2._2.isEmpty)

  println("Users Save")
  save foreach println

}


