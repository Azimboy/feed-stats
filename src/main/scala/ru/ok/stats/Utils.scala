package ru.ok.stats

import org.apache.spark.sql._

object Utils {

  implicit class SparkSessionHelpers(sparkSession: SparkSession) {
    def readTextFile(fileName: String): Dataset[String] = {
      val filePath = getClass.getClassLoader.getResource(fileName).getPath
      sparkSession
        .read
        .option("multiline", value = true)
        .textFile(filePath)
    }

    def loadTableFromOrcFile(tableName: String): Unit = {
      sparkSession.read
        .orc(s"./target/$tableName")
        .createOrReplaceTempView(tableName)
    }

  }

  implicit class SparkDataFrameHelpers(dataFrame: DataFrame) {
    def saveAsCsv(fileName: String): Unit = {
      dataFrame.coalesce(1)
        .write
        .option("header", "true")
        .option("sep", ",")
        .mode(SaveMode.Overwrite)
        .csv(s"./target/$fileName")
    }

    def saveWithRepartition(tableName: String, column: String): Unit = {
      dataFrame
        .write
        .partitionBy(column)
        .mode(SaveMode.Overwrite)
        .format("orc")
        .saveAsTable(tableName)
    }
  }

  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./target")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .appName(appName)
      .getOrCreate()
  }

}