package com.casestudy.app

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row, SparkSession}


object LoadFileHdfs extends App {

    val inputFilePath = args(0)
    val outputFilePath = args(1)

    //Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName("Telco Data Analysis")
      .enableHiveSupport()
      .getOrCreate()

    //Load telcocdr.xlsm data
    val df = spark.read
      .format("org.zuinnote.spark.office.excel")
      .load(inputFilePath)


    //Extract Header Values
    val headerRow: Seq[Any] = df.take(1)(0)(0).asInstanceOf[Seq[Any]]
    //Generate DataFrame Schema
    val schema = headerRow.map(cell => {
      val rowCell: Row = cell.asInstanceOf[Row]
      val columnName: String = rowCell(0).asInstanceOf[String]
      //Clean Column names for compatibility with Parquet Format
      val cleanColumnName = columnName.replaceAll("\\W", "")
      StructField(cleanColumnName, StringType, nullable = true)
    })

    implicit val rowEncoder = RowEncoder(StructType(schema))

    //Create DataFrame
    val df_result_map = df.map(row => {
      val rowSeq: Seq[Row] = row(0).asInstanceOf[Seq[Row]]
      val values = rowSeq.map(rowCell => rowCell(0))
      Row(values: _*)
    })

    val df_result = df_result_map.filter("CALL_DATE not like '%CALL_DATE%'")

    //Write output to FileSystem as Parquet File
    df_result.write.parquet(outputFilePath)

}




