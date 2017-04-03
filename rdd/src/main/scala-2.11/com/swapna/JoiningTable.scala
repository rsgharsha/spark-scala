package com.swapna
/**
  * Created by swapna on 3/6/17.
  emp
  ------
    1@swapna@100
    2@kiran@100
    3@vamsi@101
    4@priti@101
    
    
    
    dept
    ----
100,IT
101,HR
 
 submit through cli
 
 spark-submit --class com.swapna.JoiningTable --deploy-mode client rdds.jar
 
 create jar in intellij
 -----------------------
 
 Select the project -> File -> Project Structure -> Artifacts -> Apply -> ok
 Build-> buildArtifacts next time just rebuild
 results
 -------
 
 
 1,swapna,IT
2,kiran,IT
3,vamsi,HR
4,priti,HR
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object JoiningTable {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[*]").appName("sqltables").getOrCreate()
    val empSchema = StructType(Array(
      StructField("eid", IntegerType, true),
      StructField("ename", StringType, true),
      StructField("id", IntegerType, true)
    ))



    val deptSchema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    ))

    val emp = spark.read.format("com.databricks.spark.csv").option("delimiter", "@").schema(empSchema).load("file:///home/ubuntu/emp")
    val dept = spark.read.format("com.databricks.spark.csv").schema(deptSchema).load("file:///home/ubuntu/dept")
    emp.createOrReplaceTempView("emp")
    dept.createOrReplaceTempView("dept")
    val results = spark.sql("select e.eid,e.ename,d.name from emp e join dept d on e.id=d.id")
    results.write.format("com.databricks.spark.csv").save("file:///home/ubuntu/empdept")

  }
}
