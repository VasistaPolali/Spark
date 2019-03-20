import org.apache.spark.sql.SparkSession

trait SparkSessionTest {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}