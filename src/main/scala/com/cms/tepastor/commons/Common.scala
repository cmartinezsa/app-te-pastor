package com.cms.tepastor.commons

import org.apache.commons.cli.MissingArgumentException
import org.apache.log4j.{LogManager, Logger}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

trait Common {
  val appName = "App - TeDePastor"
  val partitionField = "date_part"
  val logger: Logger = LogManager.getLogger(appName)
  val dateFormatSimple="yyyy-MM-dd"
  val formatDateComplet = "dd/MM/yyyy HH:mm"
  val formatDateMonth = "yyyy-MM"
  val saveModeOverwrite="overwrite"
  val datePartField="date_part"
  val dateMonthField="month"
  val monthVarible="Month"
  val dayVariable="Day"
  val typeOfVaribleCol="type_variable"
  val periodColum="period"

  val tableToWrite = "cargo"
  val timestampField="timestamp"
  val numberField="number"
  val latField="lat"
  val longField="long"

  def argsChecks(args: Array[String]): Unit = {
    logger.info("Checking the number of parameters...")
    if (args.forall(p=> p.equals(""))){
      throw new MissingArgumentException("This module cant'n be executed parameterless")
    }
  }
  def substractDates(sDate:String, numberOfDays:Int): String ={
    if(sDate.isEmpty){
      throw new MissingArgumentException("Date varible is empty or null")
    }
    else {
      val localDate=LocalDate.parse(sDate, DateTimeFormatter.ofPattern(dateFormatSimple))
      localDate.minusDays(numberOfDays).toString
    }
  }
}
