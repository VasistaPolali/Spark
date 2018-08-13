# Spark

This Spark-Scala application transforms data in a XLSM file into a human readable file format and materialises it back into 
HDFS in parquet format, so that it can be queried through SQL later for reporting or visualization.

The XSLM file is a CDR (Call Details Record) file to identify the top 10 customers facing frequent call drops in Roaming. 
Telecom companies can use this report to prevent customer churn out, by calling them back and at the same time contacting their roaming partners to improve the connectivity issues in specific areas.
 

