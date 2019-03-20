import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.{StructType,StructField}

object DataTypes {

  trait DataType

  case class Basket(productId: String,grossMerchandiseValueEur: Double, productType: String) extends DataType

  case class Order(orderId: String, basket:Seq[Basket]) extends DataType

  case class CustomerOrders(customerId: String, orders: Seq[Order]) extends DataType

  case class Result(customerId: String,numOrders: Int,totalNetMerchandiseValueEur: Double) extends DataType
}
