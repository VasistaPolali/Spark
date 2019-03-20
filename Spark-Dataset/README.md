
The  application implementats an ETL job using Apache Spark Dataset API.

The Apache Spark Dataset API provides a type-safe, object-oriented programming interface. DataFrame is an alias for an untyped Dataset [Row]. Datasets provide compile-time type safety—which means that production applications can be checked for errors before they are run—and they allow direct operations over user-defined classes


It redas the json file under `/etl/src/main/resources` and calculates the _net_ merchandise value of the order(ed products) and writes the output to file on disk.

When computing the net value, the following things have been taken into consideration:
7% VAT applied to cold foods, 15% to hot foods and 9% on beverages.

To run:
sbt run "input file location" "output file location"
