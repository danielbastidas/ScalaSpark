package com.myscala.spark

import org.apache.spark.sql.SparkSession

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
      System.err.println("Usage: HdfsTest <file>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("HdfsTest")
      .getOrCreate()
//    val schema = "id DOUBLE, female STRING, ses STRING, schtyp STRING, prog STRING, read DOUBLE, write DOUBLE, " +
//      "math DOUBLE, science DOUBLE, socst DOUBLE, honors STRING, awards DOUBLE, cid INT"
    val file = spark.read.format("avro")/*.schema(schema)*/.load(args(0))
    println(s"!!!!!!!!!!!!!!Number of records: ${file.count()}")
//    println(s"!!!!!!!!!!!!!!Here comes the count: ${file.select("name", "favorite_color").show(10)}")
    println(s"!!!!!!!!!!!!!!Data: ${file.show(10)}")

    spark.stop()
  }
}

// TODO: write data in parquet format 