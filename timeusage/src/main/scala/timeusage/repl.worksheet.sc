import org.apache.spark.sql.*
import org.apache.spark.sql.types.*
import org.apache.log4j.{Logger, Level}
import scala.util.Properties.isWin

import timeusage._

val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage-REPL")
      .master("local")
      .getOrCreate()

val (columns, initDf) = TimeUsage.read("../../resources/timeusage/atussum.csv")

