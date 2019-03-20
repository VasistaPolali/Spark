import org.scalatest.FunSpec
import DataTypes.{CustomerOrders, Result}
import org.apache.spark.sql.{Row, SparkSession}

//Calss to test Spark Application
class DatasetTest extends FunSpec with SparkSessionTest {

  describe("#compare") {
    it("Asserts the Row count and values in the result set") {
      import spark.implicits._

      //Create an Dataset with Expected values
      val dsExpected = Seq(Result(
        "5b6950c0544b331cdad52877", 3, 51.3904)
      ).toDS()

      //Process test file
      val c = new ProcessSpark

      implicit lazy val sparkSession: SparkSession = c.getSparkSession(Array("args"))

      val ds = c.readData("etl/src/test/resources/test.json")
      val resultDs = c.transformData(ds)
      resultDs.show()

      //assert the row count

      assert(resultDs.count === 1)

      //assert if all the column values match between the Expected Dataset and the Actual Dataset
      assert(resultDs.except(dsExpected).count() === 0)

    }

  }
}