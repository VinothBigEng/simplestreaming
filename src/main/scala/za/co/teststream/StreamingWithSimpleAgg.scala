package za.co.teststream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}


object StreamingWithSimpleAgg {
  
  def main(args : Array[String]) {

//    Logger.getLogger("org").setLevel(Level.ERROR)

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

    import spark.implicits._
    val readSampleRS = spark.readStream
      .format("json")
      .schema(sampleDFSchema)
      .option("maxFilesPerTrigger", 1)
      .load("src/main/resources")
      /// Simple Simple and aggregation
      .selectExpr("ch_seq")
      .as[String].map(x => (x, x.length))
      .groupBy("_1")
      .agg(functions.collect_list(functions.col("_1")))

    readSampleRS
      .writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }

}
