# Spark

This Spark Application processes a CDR (Call Details Record) file to identify the top 10 customers facing frequent call drops in Roaming. 
Telecom companies can use this report to prevent customer churn out, by calling them back and at the same time contacting their
roaming partners to improve the connectivity issues in specific areas.
 
The Spark-Scala applicatio o get the above result transform the results in human readable file format and materialise them back into 
HDFS layer in parquet format so that these can be queried through SQL later for reporting or visualization.
