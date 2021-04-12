package SparkExamples

import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File
import scala.sys.exit

object DeltaTableIncrementalUpdate {
  def main(args: Array[String]): Unit = {

    //Reduce logging
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Delta directory
    val deltaDir = new File("./delta")

    if(!deltaDir.exists()) {
      println("Create the delta tables first!")
      exit(-1)
    }

    //Create spark session
    val spark = SparkSession
      .builder()
      .appName("SparkDeltaTableExample")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    //Apply incremental updates to table 1
    println("Applying incremental updates to table1")
    println("table1 before update")
    val deltaTable1 = DeltaTable.forPath(deltaDir.getCanonicalPath + "/table1")
    deltaTable1.toDF.show()

    //Read the incremental data as DataFrame
    val table1IncrementDF = spark.read.format("delta").load(deltaDir.getCanonicalPath + "/increments/table1")
    table1IncrementDF.show()

    //Filter all the rows with operation = 3, these
    val filterTable1IncrementDF = table1IncrementDF.filter("operation != 3")
    filterTable1IncrementDF.show()

    val table1ColMap = Map( "item_id" -> col("incr.item_id"),
                      "item_name" -> col("incr.item_name"),
                      "item_price" -> col("incr.item_price"))

    deltaTable1.as("table1")
               .merge(filterTable1IncrementDF.as("incr"), "table1.item_id = incr.item_id")
               .whenMatched("incr.operation = 1")
               .delete()
               .whenMatched("incr.operation = 4")
               .update(table1ColMap)
               .whenNotMatched
               .insert(table1ColMap)
               .execute()

    println("table1 after update")
    deltaTable1.toDF.show()

    //Apply incremental updates to table 2 - purposefully not-refactored
    println("Applying incremental updates to table2")
    println("table2 before update")
    val deltaTable2 = DeltaTable.forPath(deltaDir.getCanonicalPath + "/table2")
    deltaTable2.toDF.show()

    //Read the incremental data as DataFrame
    val table2IncrementDF = spark.read.format("delta").load(deltaDir.getCanonicalPath + "/increments/table2")
    table2IncrementDF.show()

    //Filter all the rows with operation = 3, these
    val filterTable2IncrementDF = table2IncrementDF.filter("operation != 3")
    filterTable2IncrementDF.show()

    val table2ColMap = Map( "id" -> col("incr.id"),
      "item_id" -> col("incr.item_id"),
      "customer_id" -> col("incr.customer_id"),
      "sold_date" -> col("incr.sold_date"),
      "discount" -> col("incr.discount"))

    deltaTable2.as("table2")
      .merge(filterTable2IncrementDF.as("incr"), "table2.id = incr.id")
      .whenMatched("incr.operation = 1")
      .delete()
      .whenMatched("incr.operation = 4")
      .update(table2ColMap)
      .whenNotMatched
      .insert(table2ColMap)
      .execute()

    println("table2 after update")
    deltaTable2.toDF.show()

    //Uncomment to pause the app, if you want to look at spark ui
    //StdIn.readLine()
  }
}
