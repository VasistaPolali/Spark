import DataTypes.{CustomerOrders, DataType, Result}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.internal.Logging

class ProcessSpark[O <: SparkApp[DataType] with DataType] extends Logging {


  def getSparkSession(args: Array[String]): SparkSession = {

    val sparkSession = SparkSession.builder.appName("datasetApp")
    //.master("local[2]")
    .getOrCreate()

    return sparkSession

  }

  def readData(filePath: String)(implicit sparkSession: SparkSession): Dataset[CustomerOrders] = {

    import sparkSession.implicits._

    //Read data in the file in to a Dataset as CustomerOrders model
    val ds = sparkSession.read.json(filePath).as[CustomerOrders]

    return ds


  }

  def transformData(in: Dataset[CustomerOrders])(implicit sparkSession: SparkSession): Dataset[Result] = {

    import sparkSession.implicits._

    //Process Dataset to count the number of orders and calculate net value based on the product type

    val ds = in.map(c => (c.customerId, c.orders.map(x => (x.orderId)).size, c.orders.map(x => x.basket.map(x => x.productType match {
      case "hot food" => x.grossMerchandiseValueEur - (x.grossMerchandiseValueEur * 15) / 100
      case "cold food" => x.grossMerchandiseValueEur - (x.grossMerchandiseValueEur * 7) / 100
      case "beverage" => x.grossMerchandiseValueEur - (x.grossMerchandiseValueEur * 9) / 100
      case _ => x.grossMerchandiseValueEur
    }
    ).sum
    ).fold(0.0)(_ + _)
    )).map(x => Result(x._1, x._2, x._3))


    return ds


  }

  def writeData(ds: Dataset[Result], outFilePath: String)(implicit sparkSession: SparkSession): Unit = {

   //Write Data as CSV
    //CSV was chosen as the Data Structure because,
    // CSV can typically be processed  incrementally, so the memory footprint stays relatively constant.
    //CSV  tends to be more compact than JSON and since the api expects the whole record related to the customerId returned in the response,
    // and the result form spark is not nested in anyways,it would perfom better if is stored in tabular fomat with a flat hierarchy.
    //Can easily be compressed in case of larger files for better peformance.


   ds.toDF("customerId", "orders", "totalNetMerchandiseValueEur").write.option("header","true").csv(outFilePath)


  }


    def main(args: Array[String]): Unit = {

      log.info("Starting spark session...")
      implicit lazy val sparkSession: SparkSession = getSparkSession(args)

      try {
        log.info("Reading data...")
        val data = readData(args(0))

        log.info("Transforming data...")
        val transformed = transformData(data)

        log.info("Writing data...")
        writeData(transformed, args(1))
      }

      finally {
        log.info("Stopping Spark session...")
        sparkSession.stop()
      }
    }


}







