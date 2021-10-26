package com.myscala.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StringIndexer

/* Compile and generate the jar using the following command: gradle jar
* from build.gradle file.
* The file build.sbt is not used */

/** This class can be executed from spark using the following command:
 spark-submit --packages org.apache.spark:spark-avro_2.12:3.1.1 --class \
"com.myscala.spark.HdfsTest" --master "local[*]" \
 "/home/danielbastidas/git-repo/Scala-Spark/build/libs/Scala-Spark-1.0.jar" \
 hdfs://localhost:9000/media/danielbastidas/HardDisk/BigData/sink/parquet-source/partition=0/parquet-source+0+0000000162+0000000164.avro
 */

/**
 spark-submit --packages org.apache.spark:spark-avro_2.12:3.1.1 --class \
 "com.myscala.spark.HdfsTest" --master "local[*]" \
 "/home/danielbastidas/git-repo/Scala-Spark/build/libs/Scala-Spark-1.0.jar" \
 hdfs://localhost:9000/media/danielbastidas/HardDisk/BigData/sink/parquet-source/partition=0
 */

object HdfsTest {

  /** Usage: HdfsTest [file] */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: HdfsTest <file> <number_of_records_to_show>")
      System.exit(1)
    }

    var numberRecordsShow = 10

    if (!args(1).toInt.isNaN) {
      numberRecordsShow = Integer.valueOf(args(1))
    }

    val spark = SparkSession
      .builder
      .appName("HdfsTest")
      .getOrCreate()
//    val schema = "id DOUBLE, female STRING, ses STRING, schtyp STRING, prog STRING, read DOUBLE, write DOUBLE, " +
//      "math DOUBLE, science DOUBLE, socst DOUBLE, honors STRING, awards DOUBLE, cid INT"
    val data = spark.read.format("avro")/*.schema(schema)*/.load(args(0))
    println(s"!!!!!!!!!!!!!!Number of records: ${data.count()}")
//    println(s"!!!!!!!!!!!!!!Here comes the count: ${file.select("name", "favorite_color").show(10)}")
//    println(s"!!!!!!!!!!!!!!Data: ${data.show(numberRecordsShow)}")

    val indexer = new StringIndexer()
      .setInputCols( Array("female", "ses", "schtyp", "prog", "honors") )
      .setOutputCols( Array("femaleFactored", "sesFactored", "schtypFactored", "progFactored", "honorsFactored"))

    val factored = indexer.fit(data).transform(data)

    println(s"!!!!!!!!!!!!!!Data2: ${factored.show(numberRecordsShow)}")

    factored.write.format("avro").save("hdfs://localhost:9000/media/danielbastidas/HardDisk/BigData/hadoop/datanode" +
      "/sink/parquet-factored")

    spark.stop()
  }
}
