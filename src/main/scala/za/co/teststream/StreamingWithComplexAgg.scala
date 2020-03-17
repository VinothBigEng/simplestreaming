package za.co.teststream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, current_timestamp, expr}
import za.co.teststream.createColStrings._

object StreamingWithComplexAgg {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.DEBUG)

    val spark = SparkSession
      .builder()
      .appName("Test Spark Streaming")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", 4)
      .master("local[*]")
      .getOrCreate()

    //read the sample json data
    val readSampleDF = spark
      .read
      .json("src/main/resources/teststream.json")

    readSampleDF.printSchema()

    //schema of the sample data
    val sampleDFSchema = readSampleDF.schema

    val stringofCase = createComplexColStrings(400, 100 )
    val manyTestCols = List(col("ch_seq") ,col("run_timestamp")) ::: stringofCase._1.map(expr(_))


//    val groupByWindowCol = functions.window($"run_timestamp" , "")

    val readSampleRS = spark.readStream
      .format("json")
      .schema(sampleDFSchema)
      .option("maxFilesPerTrigger", 1)
      .load("src/main/resources")
      .withColumn("run_timestamp", current_timestamp)
      // with complex aggregation
      .select(manyTestCols : _*)
      // Agg columns with watermark
      .withWatermark("run_timestamp", "500 milliseconds")
      .groupBy("ch_seq" ,"run_timestamp")
      .agg(functions.collect_list(functions.col( "ch_seq")))


    readSampleRS
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate","false")
      .start()
      .awaitTermination()


  }
}
