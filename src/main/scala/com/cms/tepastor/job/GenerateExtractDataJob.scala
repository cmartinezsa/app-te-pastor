package com.cms.tepastor.job
import com.cms.tepastor.commons.{Common, Reader, Writer}
import org.apache.spark.sql.functions.{col, dayofmonth, lit, month, to_date}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

class GenerateExtractDataJob  extends Common {
  val reader = new Reader();
  val writer = new Writer();

  /**
   * 
   * @param pathFileCSV
   * @param pathToSaveParquet
   * @param spark
   */
  def generateParquetFiles(pathFileCSV: String, pathToSaveParquet: String)(implicit spark: SparkSession): Unit = {
    try {
      val dfDataGeoFile = reader.readCSVFile(pathFileCSV)
        .select("*")
        .dropDuplicates()
      val dfSelect = dfDataGeoFile.printSchema()
      // Add partition field
      val dfDataGeoFileAddCol = dfDataGeoFile.withColumn(datePartField, to_date(col(timestampField), dateFormatSimple))
        .coalesce(4)
      dfDataGeoFileAddCol.show(10, false)
      //Save to Parquet File
      writer.saveResultSetAsParquet(dfDataGeoFileAddCol, saveModeOverwrite, datePartField, pathToSaveParquet)

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
    }
  }

  /**
   *
   * @param pathParquetFiles
   * @param pathOutResult
   * @param spark
   */
  def generateVariables(pathParquetFiles:String, pathOutResult:String)(implicit spark: SparkSession): Unit = {
    try {
      logger.info(s"Read from parquet path : $pathParquetFiles")

      val dfDataGeoFileParquets = reader.readParquet(pathParquetFiles)
        .select("*")
        .dropDuplicates()

      val dfDataGeoMonth= dfDataGeoFileParquets
        .withColumn(dateMonthField, month(col(datePartField)))
        .select(dateMonthField)
        .groupBy(col(dateMonthField).as(periodColum)).count().alias("count")
        .withColumn(typeOfVaribleCol,lit(monthVarible))
        .select(col(periodColum).cast("String"), col("count"),col(typeOfVaribleCol))


      val dfDataGeoDay= dfDataGeoFileParquets
        .withColumn(periodColum, dayofmonth(col(datePartField)))
        .select(periodColum)
        .groupBy(col(periodColum)).count().alias("count")
        .withColumn(typeOfVaribleCol,lit(dayVariable))

      val dfUnionVariablesGeo=dfDataGeoMonth.union(dfDataGeoDay)

      writer.saveResultSetAsParquet(dfUnionVariablesGeo, saveModeOverwrite, "type_variable", pathOutResult)

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
    }
  }
}
