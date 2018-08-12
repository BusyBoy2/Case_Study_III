package com.CaseStudySensorData.Assignment

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object SparkSensorData {

  def main(args:Array[String]): Unit = {

    //spark session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL - III")
      .getOrCreate()

    // Removing all INFO logs in consol printing only result sets
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    println("spark session object is created")

    //Schema for HVAC.csv
    val Manual_schema_HVAC = new StructType(Array(new StructField("Date", StringType, true),
      new StructField("Time", StringType, false),
      new StructField("TargetTemp", LongType, true),
      new StructField("ActualTemp", LongType, false),
      new StructField("System", LongType, false),
      new StructField("SystemAge", LongType, false),
      new StructField("BuildingID", LongType, false)))

    //Schema for building.csv
    val Manual_schema_Building = new StructType(Array(new StructField("BuildingID", LongType, true),
      new StructField("BuildingMgr", StringType, false),
      new StructField("BuildingAge", LongType, true),
      new StructField("HVACproduct", StringType, false),
      new StructField("Country", StringType, false)))

    //Reading the HVAC csv file
    val HVAC = spark.read.format("CSV").option("header",true).schema(Manual_schema_HVAC)
      .load("C:\\Users\\Shruthi\\Downloads\\HVAC.csv")

    //Displaying the dataframe contents
    HVAC.show()
    //Registering the temporary table
    HVAC.registerTempTable("HVAC_table")
    println("HVAC table registered!")

    //Reading the building.csv file and creating the temp table
    val buildings = spark.read.format("CSV").option("header", true).schema(Manual_schema_Building)
      .load("C:\\Users\\Shruthi\\Downloads\\building.csv")

    buildings.show()
    buildings.registerTempTable("building_table")
    println("buildings table registered!")


    //Approach : Using Spark SQL
    val filterHVAC = spark.sql("""select *, IF((TargetTemp-ActualTemp)> 5 ,'1',
        IF((TargetTemp-ActualTemp)< -5 ,'1',0)) as Temp_change_diff from HVAC_table""")
        filterHVAC.show()

    val joinExpression = filterHVAC.col("BuildingID") === buildings.toDF().col("BuildingID")
    val HVACJOBUILD = filterHVAC.join(buildings,joinExpression)
        HVACJOBUILD.show()
        HVACJOBUILD.registerTempTable("HVACJBUILD")

    val selective = spark.sql("""select Temp_change_diff, Country from HVACJBUILD WHERE Temp_change_diff = 1""").toDF()
    selective.registerTempTable("newselective")
    spark.sql("""select Country, count(Temp_change_diff) from newselective group by Country""").show()



  }



}
