package com.swapna

/**
  * Created by swapna on 3/6/17.R
  */
/**
  *  In this example I am getting data from mysql and saco into edshift
  *
  *  How to create redshift instance in aws
  *  ---------------------------------------
  *  1. give the username password
  *  2.select publicly accesible as yes
  *  3.choose a public address no
  *  4. I
  *  5.Install SQL workbench to connect
  *
  *  Need to download  RedshiftJdbc driver
  *
  */

//import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
//import com.amazon.redshift.jdbc.Driver


object Getting_Mysql_Info {
  def main(args:Array[String]): Unit = {
    // setting mysql driver info
    val mysql_url = "jdbc:mysql://localhost:3306/samples"
    val mysql_props = new java.util.Properties()
    mysql_props.setProperty("driver","com.mysql.jdbc.Driver")
    mysql_props.setProperty("user","root")
    mysql_props.setProperty("password","password")

    //setting redshift info
    val redshift_url = "jdbc:redshift://myredshift.ccfizqtsegpb.us-west-2.redshift.amazonaws.com:5439/dev"
    val redshift_props = new java.util.Properties()
    redshift_props.setProperty("driver","com.amazon.redshift.jdbc.Driver")
    redshift_props.setProperty("user","root")
    redshift_props.setProperty("password","Redshift123")

    val spark = SparkSession.builder().master("local[*]").appName("sqltables").getOrCreate()
    val df = spark.read.jdbc(mysql_url,"emp",mysql_props)
    df.write.jdbc(redshift_url,"emp_temp",redshift_props)


  }

}
