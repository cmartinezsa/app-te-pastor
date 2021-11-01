package com.cms.tepastor.commons


import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Properties
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame}

class Writer extends Common {
  val dateFormat = new SimpleDateFormat(formatDateComplet)
  val driver = "org.postgresql.Driver"

  /**
   *
   * @param df
   * @param saveMode
   * @param fieldPartition
   * @param pathToSave
   */
  def saveResultSetAsParquet(df: DataFrame, saveMode: String, fieldPartition:String,
                             pathToSave: String): Unit = {
    try {
      df.write
        .mode(saveMode)
        .partitionBy(fieldPartition)
        .parquet(pathToSave)

      logger.info("The Data save into the path : ".concat(pathToSave))
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check table about $ex ")
    }
  }

  /**
   *
   * @param df
   * @param saveOption
   * @param jdbcUrl
   * @param jdbcUser
   * @param jdbcPassword
   * @param tableToWrite
   */
  def saveResultJDBC(df: DataFrame, saveOption: String, jdbcUrl: String,
                     jdbcUser: String, jdbcPassword: String,
                     tableToWrite: String): Unit = {

    println(" Metodo saveResultJDBC --> ")
    println("saveOption " + saveOption )
    println("jdbcUrl " + jdbcUrl )
    println("user " +jdbcUser )
    println("password :" + jdbcPassword )
    println("Tabla: " + tableToWrite )
    val connectionProperties = new Properties
    connectionProperties.put("user", jdbcUser)
    connectionProperties.put("password", jdbcPassword)
    try {
      df.write
        .mode(saveOption)
        .jdbc(jdbcUrl, tableToWrite, connectionProperties)
      logger.info("WRITE TO POSTGRES SUCESSFULL")
    }
    catch {
      case ex: IOException =>
        logger.error(s"Check the path about $ex ")
      case psql: org.postgresql.util.PSQLException=>
        logger.error(s"Check the database conecction $psql ")

    }
  }
}