import org.apache.spark.SparkContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, Row}

object SparkIngestion {

  val jdbcURL="jdbc:mysql://localhost:3306/hadoop"
  val username="sarthak"
  val password="sarthak"
  val mysqlTable="aggregation"
  val hdfsPath="hdfs://127.0.0.1:9000/spark/dataset.csv"
  val mysqlDB="hadoop"

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  Class.forName("com.mysql.jdbc.Driver").newInstance
  val connectionProperties = new Properties()

  connectionProperties.put("user", "sarthak")
  connectionProperties.put("password", "sarthak")

  val con:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/"+mysqlDB,connectionProperties)

  val sc = new SparkContext("local[*]" , "Spark")
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def createTable(): Unit ={

    val result = con.prepareStatement(s"create table if not exists aggregation (mode varchar(255), median integer) ").execute()

    if (result==true)
      println("Table created")
    else
      println("Table not created")

  }

  def medianCalculator(seq: Seq[Int]): Int = {
    //In order if you are not sure that 'seq' is sorted
    val sortedSeq = seq.sortWith(_ < _)

    if (seq.size % 2 == 1) sortedSeq(sortedSeq.size / 2)
    else {
      val (up, down) = sortedSeq.splitAt(seq.size / 2)
      (up.last + down.head) / 2
    }
  }

  def writeDFtoSqlTable(aggregation: DataFrame) = {
    aggregation
      .write
      .format("jdbc")
      .mode("overwrite")
      .option("url", jdbcURL)
      .option("dbtable", mysqlTable)
      .option("user", username)
      .option("password", password)
      .save()
  }

  def readDataFromSqlTable(): Unit ={
    val data = sqlContext.read
      .format("jdbc")
      .option("url", jdbcURL)
      .option("user", username)
      .option("password", password)
      .option("dbtable", mysqlTable)
      .load()
    data.show()
  }

  def main(args : Array[String]): Unit ={

    createTable()

    // Read dataset from csv on hdfs
    val hadoopDataset = sqlContext.read.option("header", "true").csv(hdfsPath)
    hadoopDataset.registerTempTable("dataset")

    //Read from dataset table and get values for Median and Mode
    val mode=sqlContext.sql("SELECT `name` FROM `dataset` GROUP BY `name` ORDER BY count(number) DESC LIMIT 1").first().get(0)
    val numbers=sqlContext.sql("SELECT `number` FROM `dataset` ORDER BY number").collect().map(_.get(0)).toList
    val median=medianCalculator(numbers.map(_.toString.toInt))

    //Create aggregation dataframe
    val aggregation = Seq(
      (median.toString,mode.toString)
    ).toDF("Median", "Mode")

    //Write data df to sql table
    writeDFtoSqlTable(aggregation)
    println()
    println("Dataframe written to sql table "+mysqlTable)

    println("Reading from sql table "+mysqlTable)
    //Read from Sql Table
    readDataFromSqlTable()

  }
}