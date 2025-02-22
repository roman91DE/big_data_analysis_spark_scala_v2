package io

import java.nio.file.{FileSystems, Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.io.Source
import scala.compiletime.ops.int


class Reader(path: String):
  val pathObj = Paths.get(path)
  def exists(): Boolean = Files.exists(pathObj)

  def parseFile(): Option[List[String]] = {
    if !exists() then None else {
      val p = pathObj.toAbsolutePath().toString()
      val source = Source.fromFile(p)
      Some(source.getLines.toList)
    }
  }

def globber(pattern: String): List[Option[List[String]]] = {
    val matcher = FileSystems.getDefault.getPathMatcher(pattern)
    val startDir = Paths.get("./data")

    val matches = Files.walk(startDir).iterator().asScala
      .filter(path => matcher.matches(path))

    matches.map(p => Reader(p.toString()).parseFile()).toList

}



@main def hello(): Unit =
  val data = globber("glob:**/*.txt").flatMap(_.getOrElse(Nil)).flatMap(_.split(" "))

  val kv = data.map(w => (w, 1))
  
  val res = kv.groupMapReduce(_._1)(_._2)((a,b) => a+b).toList.sortBy(_._2)(Ordering.Int.reverse)

  res foreach println


  
  