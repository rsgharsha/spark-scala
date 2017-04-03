/**
  * Created by kiranmeda on 2/14/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object bank_rdd_df {

  val spark=SparkSession.builder().master("").appName("").getOrCreate()
  import spark.implicits._
  //val sc=spark.sparkContext
  val bank=spark.read.textFile("file:///home/ubuntu/bank.csv")
  val heder=bank.first()
  val bankrdds=bank.filter(x=>x!=heder).map(x=>(x.split(";")(0),x.split(";")(2).replaceAll("\"",""),x.split(";")(5))).toDF("age","marital","bal")
  bankrdds.createOrReplaceTempView("bank")
  spark.sql("select * from bank where marital='single' order by age desc limit 1").collect



  //val heder=bank.first()
  //case class bankclss(age:Int,marital:String,bal:Int)
  //val bankrdds=bank.filter(x=>x!=heder).map(x=>(x.split(";"))).
  //val bankrdds=bank.filter(x=>x!=heder).map(x=>(x.split(";"))).map(x=>bankclss(x(0).toInt,x(2).replaceAll("\"",""),x(5).replaceAll("\"","").toInt))
 // bankrdds.toDF().registerTempTable("samp")
  //sql("select * from samp limit 1").collect
}
