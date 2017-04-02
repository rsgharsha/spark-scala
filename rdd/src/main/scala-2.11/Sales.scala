/**
  * Created by swapna on 2/28/17.
  */

/**

one of the record has 13,000 has price so to remove , need to apply udf
*/

object Sales {
  val df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load("file:///home/ubuntu/Sales.csv")
  import org.apache.spark.sql.functions._
  val newDF = df.withColumn(df.columns(2), regexp_replace(col(df.columns(2)), ",", ""))


}


/*
using case clss

 case class mydata (Transaction_date: String,Product: String,Price:Int,Payment_Type:String,Name:String,City:String,State:String,Country:String,Account_Created:String,Last_Login:String,Latitude:String,Longitude:String)

  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("feb16first").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("feb16first").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val rss = sc.textFile(args(0))
    val head = rss.first()
    val myfile = rss.filter(x=>x!=head).map(x=>x.replaceAll("\"(.*),(.*)\"","$1$2")).map(x=>x.split(",")).map(x=> mydata(x(0),x(1),x(2).toInt,x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11)))
    val mycase = myfile.toDF()
    mycase.printSchema()
    mycase.show(5)
    mycase.createOrReplaceTempView("att")
    spark.sql("select * from att").write.format("com.databricks.spark.csv").option("header","true").save(args(1))
    //spark.sql("select avg(Price) from att where Product = 'Product1' and 'Product2").show()
    //spark.sql("select avg(Price) from att where Product = 'Product2'").show()
    spark.stop()
  }
}


 */
