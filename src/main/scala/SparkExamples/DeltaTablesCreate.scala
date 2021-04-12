package SparkExamples

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.io.StdIn

object DeltaTablesCreate {
  def main(args: Array[String]): Unit = {

    //Delta directory
    val deltaDir = new File("./delta")

    //Create spark session
    val spark = SparkSession
      .builder()
      .appName("SparkDeltaTableExample")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    //Cleanup
    cleanDir(deltaDir)

    //Create tables
    createDeltaTableFromTable1And2CSVs(spark, deltaDir)

    //Create the incremental updates delta table
    createIncrementalUpdateTables(spark, deltaDir)

    //Uncomment to pause the app, if you want to look at spark ui
    //StdIn.readLine()
  }

  def createDeltaTableFromTable1And2CSVs(spark: SparkSession, d: File) = {
    //Read the table 1 and 2 into data frames
    val table1Df = readCsvWithHeaders(spark, getClass.getResource("/table1").getPath)
    val table2Df = readCsvWithHeaders(spark, getClass.getResource("/table2").getPath)

    //Write to delta tables
    table1Df.write.format("delta").save(d.getCanonicalPath + "/table1")
    table2Df.write.format("delta").save(d.getCanonicalPath + "/table2")
  }

  /*
    SQL CDC will capture the changed rows with following operation codes

    https://www.c-sharpcorner.com/article/change-data-capture-cdc-in-sql-server/

    operation = 1 denotes deleted rows
    operation = 2 denotes new inserted rows
    operation = 3 denotes row before the update
    operation = 4 denotes row after the update

    Updates
    Table 1
    Item with id 4 updated
    Item with id 5 deleted
    Item with id 16 inserted

    Table 2
    Row corresponding to item id 5 deleted
    Row corresponding to item id 16 inserted
  */
  def createIncrementalUpdateTables(spark: SparkSession, d: File) = {
    val table1IncrDf = readCsvWithHeaders(spark, getClass.getResource("/increments/table1").getPath)
    val table2IncrDf = readCsvWithHeaders(spark, getClass.getResource("/increments/table2").getPath)

    //Write to delta tables
    table1IncrDf.write.format("delta").save(d.getCanonicalPath + "/increments/table1")
    table2IncrDf.write.format("delta").save(d.getCanonicalPath + "/increments/table2")
  }

  def cleanDir(d: File) = {
    if (d.exists())
      FileUtils.deleteDirectory(d)
  }

  def readCsvWithHeaders(spark:SparkSession, path:String) : DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }
}
