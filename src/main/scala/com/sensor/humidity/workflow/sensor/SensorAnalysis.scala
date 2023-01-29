package com.sensor.humidity.workflow.sensor

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, when}


object SensorAnalysis {

  var csvFileProcessed = 0
  
  def main(args: Array[String]): Unit = {
    try {

      val directory = args(0)

      val applicationName = "csv file reading"

      val sparkConf = new SparkConf()

      val spark = SparkSession.builder().master("yarn").appName(applicationName)
        .config("spark.sql.warehouse.dir", "file:/user/hive/warehouse/")
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()

      fileReader(spark,directory)
      stdOutMessage(spark,directory)
      minMaxAvg(spark)
    } catch {
      case e: Exception =>
        e.printStackTrace();
    }
  }

  /*Reusable function*/
  def getAggFunc(colName: String, aggName: String, alias: String = ""): Column = {

    var aggFun = aggName match {
      case "max" => org.apache.spark.sql.functions.max(colName).alias(alias)
      case "min" => org.apache.spark.sql.functions.min(colName).alias(alias)
      case "avg" => org.apache.spark.sql.functions.avg(colName).cast("integer").alias(alias)

    }
    return aggFun
  }

  def fileReader(spark :SparkSession,directory:String): Unit= {
    
    val df1 = spark.read.option("header", "true").csv(directory+"/*.csv")
    
    df1.createOrReplaceTempView("Tablecsv")
  }
  
  def stdOutMessage(spark :SparkSession,directory:String): Unit={
   
    val tabledf=spark.sql("select * from Tablecsv")

    import org.apache.hadoop.fs._

    val conf = spark.sparkContext.hadoopConfiguration
    
    val gcsBucket = new Path(directory)
    
    val filesIter = gcsBucket.getFileSystem(conf).listFiles(gcsBucket, true)
    
    var files = Seq[Path]()

    while (filesIter.hasNext) {
     
      files = files :+ filesIter.next().getPath
     
      csvFileProcessed=csvFileProcessed+1
    }

    println("Num of processed files: "+ csvFileProcessed)
    println("Num of processed measurements: "+ tabledf.filter(col("humidity") =!= "NaN").count())
    println("Num of failed measurements: "+ tabledf.filter(col("humidity") === "NaN").count())
  }

  def minMaxAvg(spark :SparkSession): Unit= {
    val Tableview = spark.sql("select * from Tablecsv")

    val processedSensorDf = Tableview.filter(col("humidity") =!= "NaN").groupBy("sensorid").agg(getAggFunc("humidity", "min", "Min")
        , getAggFunc("humidity", "max", "Max")
        , getAggFunc("humidity", "avg", "Avg")).orderBy(col("Avg").desc)

    val failedSensorDf=Tableview.groupBy("sensorid").agg(getAggFunc("humidity", "min", "Min")
        , getAggFunc("humidity", "max", "Max")
        , getAggFunc("humidity", "avg", "Avg")).where(col("Min")==="NaN")

    val ProcFailedUnionDf= Seq(processedSensorDf,failedSensorDf)

    val sensorAggDf=ProcFailedUnionDf.reduce(_.union(_))

    val replaceDf=sensorAggDf.withColumn("Avg",when(col("Avg") === 0,"NaN").otherwise(col("Avg")))

    replaceDf.show(9,false)

  }

}
