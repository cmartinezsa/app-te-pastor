package com.cms.tepastor.joboperator
import com.cms.tepastor.commons.Common
import org.apache.spark.sql.SparkSession
import com.cms.tepastor.job.GenerateExtractDataJob


object TePastorOperator extends Common{
val genExtData= new GenerateExtractDataJob

  def main(args: Array[String]): Unit = {
    argsChecks(args)
    val pathFileCSV=args(0)
    val numberOfDays=args(1).toInt
    val pathFileParquet=args(2)

  logger.info(s"Argumentos recibidos ${args.mkString("|")}")

    logger.info("Inicializando SparkSession")
    val spark:SparkSession=SparkSession.builder()
      .appName(appName)
      .config("spark.service.user.postgresql.pass", "my_password")
      .config("spark.service.user.postgresql.user", "my_user_db")
      .config("spark.service.user.postgresql.database", "db_test_daen")
      .config("spark.service.user.postgresql.port", "5432")
      .config("spark.service.user.postgresql.host", "localhost")
      .master("local[*]") // Cambiarlo por el entorno
      .getOrCreate()
    //spark.sparkContext.setLogLevel("WARN")

    spark.conf.set("spark.sql.sources.partitionOverWriteMode", "dynamic")

    logger.info(s"SparkSession creado ${spark.sessionState}")

    // Solamente para obtener los datos parquet y probar.
    genExtData.generateParquetFiles(pathFileCSV, pathFileParquet)(spark)
    // Proceso de generacion de variables.
    genExtData.generateVariables("src/main/resources/out/", "src/main/resources/result")(spark)

    spark.stop()
    logger.info("Finish App")
  }

}
