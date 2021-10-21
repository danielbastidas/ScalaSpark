package com.myscala.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkFiles

object DataFrameFactorer {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame Factorer")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    //TODO read the file as a data frame
    //TODO factor/string index the required columns
    //TODO save the dataframe on spark

//    val urlFile="https://stats.idre.ucla.edu/stat/data/hsbdemo.dta"
//    spark.sparkContext.addFile(urlFile)
//    val originalDf = sc.textFile("file://"+SparkFiles.get("hsbdemo.dta"))

    val originalDf = spark.read.format("binaryFile").load("/home/danielbastidas/Downloads/hsbdemo.dta")
//      spark.read.format("dta").load("https://stats.idre.ucla.edu/stat/data/hsbdemo.dta")
    println("HERE IT GOES!!!!!!!!!!!!!!!!!!")
    originalDf.show(6)
    originalDf.printSchema()
    println("END OF HERE IT GOES!!!!!!!!!!!!")
  }
}
