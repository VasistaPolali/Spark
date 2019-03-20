
//Main object to run Spark job
object Main extends App{

  val data = new ProcessSpark
  data.main(Array(args(0),args(1)))


}


