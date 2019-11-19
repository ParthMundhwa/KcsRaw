package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.elasticsearch.spark._ 
import org.elasticsearch.spark.rdd.EsSpark  
import scala.io.Source
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions._

object GitStream {


  
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().appName("StreamData").getOrCreate()
    val sqlContext = spark.sqlContext 
    import sqlContext.implicits._
    
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kcs-soyou-stnn:9092").option("startingOffsets","earliest").option("subscribe", "realtime").load()
    val dataindataframe = df.selectExpr("CAST(value AS STRING)").as[(String)]
    val schema = new StructType().add("Date", StringType).add("Time", StringType).add("Date_Time", StringType).add("Parameter_Name", StringType).add("Ending_Range", StringType).add("Machine_Status", StringType).add("Machine_MAC_Address", StringType).add("Calibration_Name", StringType).add("Unit", StringType).add("Notification_Message", StringType).add("City", StringType).add("District", StringType).add("Starting_Range", StringType).add("Analyzer", StringType).add("State", StringType).add("Last_Configued_Date", StringType).add("Parameter_Value", StringType).add("Industry_Type", StringType).add("Station_Name", StringType).add("Parameter_Desc", StringType).add("Machine_name", StringType).add("Latitude", StringType).add("Calibrator", StringType).add("Parameter_Type", StringType).add("Longitude", StringType).add("Machine_Type", StringType).add("Fact_Id", StringType).add("Machine_IP_Address", StringType).add("Industry_Name", StringType).add("Industry_Id", StringType).add("Industry_Type_Id", StringType).add("Parameter_Id", StringType).add("Parameter_Type_Id", StringType).add("Unit_Id", StringType).add("Range_Id", StringType).add("Station_Id", StringType)
    val separateColumnsDataFrame = dataindataframe.select(from_json(col("value").cast("string"), schema).as("data")).select("data.*")
    val RemoveDf = separateColumnsDataFrame.drop("Machine_status","Machine_MAC_address","Last_Configued_Date","Machine_name","Machine_type","Machine_IP_Address")
    val whitespace = RemoveDf.na.fill("null",Seq("Calibration_Name","Calibrator","Analyzer","Notification_Message")).na.replace(Seq("Calibration_Name","Calibrator","Analyzer","Notification_Message"),Map("null"->"NA"))
    //whitespace.writeStream.format("console").start()
    whitespace.writeStream.outputMode("append").format("org.elasticsearch.spark.sql").option("checkpointLocation","RealtimeCheckPoint").option("es.port","9200").option("es.nodes","172.16.2.12").start("gpcbtransformation/Gpcb_type").awaitTermination()
    
  }
  

}