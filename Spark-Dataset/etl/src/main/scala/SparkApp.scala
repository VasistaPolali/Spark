import org.apache.spark.sql.{Dataset, SparkSession}
import DataTypes._
import org.apache.spark.internal.Logging

trait SparkApp[O <: DataType] extends Logging {

  // method that starts spark
  def getSparkSession(args: Array[String]): SparkSession = {

    val sparkSession = SparkSession.builder.appName("dataEngineeringChallenge").master("local[2]").getOrCreate()

    return sparkSession

  }

  // read data from resources/data.json into spark Dataset
  def readData()(implicit sparkSession: SparkSession): Dataset[CustomerOrders]

  // optionally apply transformations to data before persisting it to hive
  def transformData(in: Dataset[CustomerOrders])(implicit sparkSession: SparkSession): Dataset[O]

  // write final dataset to hive
  def writeData(ds: Dataset[O])(implicit sparkSession: SparkSession): Unit

  // Final entrypoint method
  final def main(args: Array[String]): Unit = {

    log.info("Starting spark session...")
    implicit lazy val sparkSession: SparkSession = getSparkSession(args)

    try {
      log.info("Reading data...")
      val data = readData()

      log.info("Transforming data...")
      val transformed = transformData(data)

      log.info("Writing data...")
      writeData(transformed)
    }

    finally {
      log.info("Stopping Spark session...")
      sparkSession.stop()
    }
  }

}


