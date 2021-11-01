package com.cms.tepastor.commons

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

class Reader extends  Common {

  /**
   *
   * @param pathFile
   * @param spark
   * @return
   */
  def readCSVFile(pathFile: String)(implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    try {
      val df = spark.read
        .option("inferSchema", "true") // Make sure to use string version of true
        .option("header", true)
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss.SSSS")
        .option("sep", ",")
        .csv(pathFile)

      if (!df.isEmpty){
        logger.info("**** El Dataframe contiene datos")
        df

      }else{
        logger.info("**** El Dataframe no contiene datos ")
        dfEmpty
      }

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }

  /**
   *
   * @param pathFile
   * @param spark
   * @return
   */
  def readParquet(pathFile: String)(implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    try {
      val df = spark.read
        .parquet(pathFile)
      //  .where(col(partitionField).between(startDate, endDate))

      if (!df.isEmpty){
        logger.info("**** El Dataframe contiene datos")
        df

      }else{
        logger.info("**** El Dataframe no contiene datos ")
        dfEmpty
      }

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }

}
