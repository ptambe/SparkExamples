package SparkExamples

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn

object ShuffleJoinExample {
  def main(args: Array[String]): Unit = {

    //Create spark session
    val spark = SparkSession
      .builder()
      .appName("SparkShuffleJoinExample")
      .master("local[3]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    //Disabling the auto broadcast join - if we remove this spark will do a BroadcastMerge Join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    //Read the table 1 and 2 into data frames
    val table1Df = readCsvWithHeaders(spark, getClass.getResource("/table1").getPath)
    //table1Df.show()

    val table2Df = readCsvWithHeaders(spark, getClass.getResource("/table2").getPath)
    //table2Df.show()

    //Join the two Data frames - this will use the sort merge join
    val resultDf = table1Df.join(table2Df, table1Df("item_id") === table2Df("item_id"))

    //Force eval
    //resultDf.foreach(_ => ())
    resultDf.show()

    //Stopping the code - just so that we can look at spark ui
    StdIn.readLine()
  }

  def readCsvWithHeaders(spark:SparkSession, path:String) : DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }


}
