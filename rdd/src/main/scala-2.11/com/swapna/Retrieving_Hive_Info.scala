package com.swapna

/**
  * Created by swapna  on 3/7/17.
  */
// prerequisites
// copy hive-site.xml,hdfs-site.xml,core-site.xml to spark/conf
// edit spark-env.sh
//export CLASSPATH="$CLASSPATH:/home/ubuntu/work/spark-2.1.0-bin-hadoop2.7/jars/mysql-connector-java-5.1.40-bin.jar"
import org.apache.spark.sql.SparkSession
object Retrieving_Hive_Info {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("").config("spark.sql.warehouse.dir", "file:///home/ubuntu").enableHiveSupport().getOrCreate()
    spark.sql("show databases").show()

  }
}
